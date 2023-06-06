#ifndef SHM_MAP_H
#define SHM_MAP_H

#include <stdio.h>
#include <time.h>

#include <atomic>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <vector>

#include "./shm_pool.h"

namespace ShmMap {

using namespace boost::interprocess;
const std::string BUCKET = "_bucket";
const std::string BUCKET_SIZE = "_bucket_size";
const std::string GARBAGE_LIST_HEAD = "_garbage_head";
const std::string GARBAGE_LIST_TAIL = "_garbage_tail";
const uint32_t DEBFAULT_BUCKET_SIZE = 1024;

template <typename Key, typename Value>
struct ItemNode {
  uint64_t _next;
  Key _key;
  Value _value;
  volatile int _expire;

  std::atomic<int>
      _invalid;  // 0 - valid  1 - add garbage list  2 - should to delete
  uint64_t _del_next;
};

struct BucketItem {
  std::atomic<uint32_t> _count;

  uint64_t _head;
  std::atomic<uint64_t> _tail;

  BucketItem() {
    _count = 0;
    _head = OFFSET_NULL;
    _tail = OFFSET_NULL;
  }
};

struct NsCalcTool {
  std::atomic<uint64_t> _count;
  std::atomic<uint64_t> _time;

  // just test for performance, don't care the thread safe problem
  uint64_t AverageCost() {
    if (_count.load(std::memory_order_consume) == 0) {
      return 0;
    }

    uint64_t average = _time.load(std::memory_order_consume) /
                       _count.load(std::memory_order_consume);
    return average;
  }

  void Calc(struct timespec &begin_, struct timespec &end_) {
    uint64_t cost = end_.tv_sec * 1000000000 + end_.tv_nsec -
                    begin_.tv_sec * 1000000000 - begin_.tv_nsec;
    _count.fetch_add(1, std::memory_order_acq_rel);
    _time.fetch_add(cost, std::memory_order_acq_rel);
  }
};

enum SinHashRet {
  RET_OK = 0,
  RET_NOT_FOUND = 1,
  RET_NO_MEMORY = 2,
};

enum ItemStatus {
  VALID = 0,
  COLLECTING = 1,
  WAITING_DELETE = 2,
  WRITING = 3,
};

#define Item ItemNode<Key, Value>

template <typename Key, typename Value>
class ShmHashMap {
 public:
  explicit ShmHashMap(std::string name, ShmPool::MemoryPool<Item> *pool,
                      managed_shared_memory *segment,
                      uint32_t bucket_size = DEBFAULT_BUCKET_SIZE);

  virtual ~ShmHashMap();

  int Insert(const Key &key, const Value &value, int expire = 0);

  int Get(const Key &key, Value &value);

  int GetCount();

  int GetAllValues(std::vector<Value> &values);

  int GetAllKeys(std::vector<Key> &keys);

  void GC();

 protected:
  void *Allocate();

  void Free(Item *ptr);

  virtual uint32_t HashCode(const Key &key) = 0;

 private:
  void SafeFree();

  void Scan();

  void AddGarbageList(Item *node);

  void RemoveExpireNode(Item *p, BucketItem &bucket);

  int AddNodeItem(int index, const Key &key, const Value &value, int expire);

  Item *GetNode(uint32_t index, const Key &key);

  Item *OffsetToNode(uint64_t);

  uint64_t NodeToOffset(Item *node);

  bool CheckDoubleFree(Item *node);

  Item *NextNode(uint64_t);

  Item *NextDelNode(uint64_t);

  uint32_t _bucket_size;
  std::atomic<uint32_t> _gc_timestamp;

  ShmPool::MemoryPool<Item> *_pool;
  managed_shared_memory *_segment;
  std::string _name;

  // TODO: use a lock-free class implement
  // garbage list (one thread add and remove)
  uint64_t *_garbage_list_head_offset;
  uint64_t *_garbage_list_tail_offset;
  BucketItem *_buckets;
};

// implements
template <typename Key, typename Value>
ShmHashMap<Key, Value>::ShmHashMap(std::string name,
                                   ShmPool::MemoryPool<Item> *pool,
                                   managed_shared_memory *segment,
                                   uint32_t bucket_size) {
  if (bucket_size == 0) bucket_size = DEBFAULT_BUCKET_SIZE;

  _segment = segment;
  _name = name;
  _pool = pool;
  _bucket_size = bucket_size;

  _buckets = _segment->find_or_construct<BucketItem>(
      (name + BUCKET).c_str())[bucket_size]();
  _garbage_list_head_offset = _segment->find_or_construct<uint64_t>(
      (name + GARBAGE_LIST_HEAD).c_str())(OFFSET_NULL);
  _garbage_list_tail_offset = _segment->find_or_construct<uint64_t>(
      (name + GARBAGE_LIST_TAIL).c_str())(OFFSET_NULL);
}

template <typename Key, typename Value>
ShmHashMap<Key, Value>::~ShmHashMap() {
  if (_buckets != NULL) {
    for (int i = 0; i < _bucket_size; ++i) _buckets[i].~BucketItem();
    _segment->destroy<BucketItem>((_name + BUCKET).c_str());
  }

  _buckets = NULL;
}

// offset to Item
template <typename Key, typename Value>
Item *ShmHashMap<Key, Value>::OffsetToNode(uint64_t offset) {
  return _pool->GetObjByOffset(offset);
}

template <typename Key, typename Value>
Item *ShmHashMap<Key, Value>::NextNode(uint64_t offset) {
  return OffsetToNode(OffsetToNode(offset)->_next);
}

template <typename Key, typename Value>
Item *ShmHashMap<Key, Value>::NextDelNode(uint64_t offset) {
  return OffsetToNode(OffsetToNode(offset)->_del_next);
}

template <typename Key, typename Value>
uint64_t ShmHashMap<Key, Value>::NodeToOffset(Item *node) {
  return _pool->GetOffsetByObj(node);
}

template <typename Key, typename Value>
int ShmHashMap<Key, Value>::Insert(const Key &key, const Value &value,
                                   int expire) {
  int index = HashCode(key) % _bucket_size;

  Item *item = GetNode(index, key);

  if (item == NULL || (item->_expire != 0 && item->_expire < time(NULL))) {
    if (AddNodeItem(index, key, value, expire) == RET_NO_MEMORY) {
      return RET_NO_MEMORY;
    }
  } else {
    int invalid = VALID, write = WRITING;

    // lock the item first
    while (!item->_invalid.compare_exchange_strong(invalid, write,
                                                   std::memory_order_acq_rel)) {
      if (item->_invalid.load(std::memory_order_acquire) == WRITING) {
        // writing occur, so wait
        invalid = VALID, write = WRITING;
        continue;
      } else {
        // item is add to garbage list
        if (AddNodeItem(index, key, value, expire) == RET_NO_MEMORY)
          return RET_NO_MEMORY;
      }
    }

    item->_value = value;
    item->_expire = expire != 0 ? time(NULL) + expire : 0;
    item->_invalid.store(VALID, std::memory_order_release);
  }
  return RET_OK;
}

template <typename Key, typename Value>
int ShmHashMap<Key, Value>::Get(const Key &key, Value &value) {
  int index = HashCode(key) % _bucket_size;
  Item *item = GetNode(index, key);

  if (item == NULL || (item->_expire != 0 && item->_expire < time(NULL))) {
    return RET_NOT_FOUND;
  }

  value = item->_value;
  return RET_OK;
}

template <typename Key, typename Value>
int ShmHashMap<Key, Value>::GetAllValues(std::vector<Value> &values) {
  for (int i = 0; i < _bucket_size; ++i) {
    BucketItem &bucket = _buckets[i];
    Item *p = OffsetToNode(bucket._head);

    while (p != NULL) {
      values.push_back(p->_value);
      p = OffsetToNode(p->_next);
    }
  }

  return RET_OK;
}

template <typename Key, typename Value>
int ShmHashMap<Key, Value>::GetAllKeys(std::vector<Key> &keys) {
  for (int i = 0; i < _bucket_size; ++i) {
    BucketItem &bucket = _buckets[i];
    Item *p = OffsetToNode(bucket._head);

    while (p != NULL) {
      keys.push_back(p->_key);
      p = OffsetToNode(p->_next);
    }
  }

  return RET_OK;
};

template <typename Key, typename Value>
int ShmHashMap<Key, Value>::GetCount() {
  int sum = 0;
  for (int i = 0; i < _bucket_size; ++i)
    sum += _buckets[i]._count.load(std::memory_order_consume);

  return sum;
}

template <typename Key, typename Value>
void ShmHashMap<Key, Value>::GC() {
  /* two steps:
      1. scan expire ItemNode, push into garbage list
      2. clear ItemNode-s whose cal more than 1 in garbage list

      the two steps need pause at least BREAK_TIME seconds
  */

  const int BREAK_TIME = 2;
  uint32_t last_timestamp = _gc_timestamp.load(std::memory_order_acquire);

  // for better performance using cas weak version
  uint32_t now = time(0);
  if (last_timestamp + BREAK_TIME < time(NULL) &&
      _gc_timestamp.compare_exchange_weak(last_timestamp, now,
                                          std::memory_order_release)) {
    Scan();
    SafeFree();
  }
}

template <typename Key, typename Value>
void ShmHashMap<Key, Value>::SafeFree() {
  Item *p0 = OffsetToNode(*_garbage_list_head_offset);
  if (p0 == NULL) {
    return;
  }

  Item *p1 = OffsetToNode(p0->_del_next);

  while (p1) {
    if (p1->_invalid.fetch_add(1, std::memory_order_acq_rel) == 2) {
      p0->_del_next = p1->_del_next;
      p1->~Item();
      Free(p1);
      p1 = OffsetToNode(p0->_del_next);
    } else {
      p0 = p1;
      p1 = OffsetToNode(p1->_del_next);
    }
  }
  *_garbage_list_tail_offset = NodeToOffset(p0);
}

template <typename Key, typename Value>
bool ShmHashMap<Key, Value>::CheckDoubleFree(Item *node) {
  Item *p0 = OffsetToNode(*_garbage_list_head_offset);
  if (p0 == NULL) {
    return false;
  }

  if (p0 == node) return true;

  Item *p1 = OffsetToNode(p0->_del_next);

  while (p1) {
    if (p1 == node) return true;

    p1 = OffsetToNode(p1->_del_next);
  }

  return false;
}

template <typename Key, typename Value>
void ShmHashMap<Key, Value>::Scan() {
  for (int i = 0; i < _bucket_size; ++i) {
    BucketItem &bucket = _buckets[i];

    Item *p0 = OffsetToNode(bucket._head);

    if (p0 == NULL) continue;

    Item *p1 = OffsetToNode(p0->_next);

    while (p1 != NULL && p1->_next != OFFSET_NULL) {
      if (p1->_expire && p1->_expire < time(NULL)) {
        // find expire ItemNode

        int valid = VALID;
        int collecting = COLLECTING;

        // lock the item
        if (p1->_invalid.compare_exchange_strong(valid, collecting,
                                                 std::memory_order_acq_rel)) {
          RemoveExpireNode(p1, bucket);

          // remove the node
          p0->_next = p1->_next;
          p1 = OffsetToNode(p0->_next);
        } else if (p1->_invalid.load(std::memory_order_consume) ==
                       (int)COLLECTING &&
                   p1->_expire < time(NULL) - 10) {
          if (!CheckDoubleFree(p1)) {
            RemoveExpireNode(p1, bucket);
          }

          // remove the node
          p0->_next = p1->_next;
          p1 = OffsetToNode(p0->_next);
        }

      } else {
        p0 = p1;
        p1 = OffsetToNode(p1->_next);
      }
    }

    // check head invalid
    p0 = OffsetToNode(bucket._head);
    if (p0->_expire && p0->_expire < time(NULL)) {
      int valid = VALID;
      int collecting = COLLECTING;

      // lock the head
      if (p0->_invalid.compare_exchange_strong(valid, collecting,
                                               std::memory_order_acq_rel)) {
        RemoveExpireNode(p0, bucket);

        if (bucket._head == bucket._tail.load(std::memory_order_consume)) {
          uint64_t expected = bucket._head, desire = OFFSET_NULL;
          if (bucket._tail.compare_exchange_strong(expected, desire,
                                                   std::memory_order_acq_rel)) {
            bucket._head = OFFSET_NULL;
          } else {
            bucket._head = expected;
          }
        } else {
          bucket._head = p0->_next;
        }
      }
    }
  }
}

template <typename Key, typename Value>
void ShmHashMap<Key, Value>::AddGarbageList(Item *node) {
  if (*_garbage_list_head_offset == OFFSET_NULL) {
    *_garbage_list_head_offset = NodeToOffset(node);
    *_garbage_list_tail_offset = NodeToOffset(node);
  } else {
    OffsetToNode(*_garbage_list_tail_offset)->_del_next = NodeToOffset(node);
    *_garbage_list_tail_offset = NodeToOffset(node);
  }
}

template <typename Key, typename Value>
void ShmHashMap<Key, Value>::RemoveExpireNode(Item *p, BucketItem &bucket) {
  //    // (1) atomic change invalid to 1
  //    p->_invalid.store(1, std::memory_order_release);

  // (2) add garbage list
  AddGarbageList(p);

  // (3) count reduce 1
  bucket._count.fetch_sub(1, std::memory_order_acq_rel);
}

template <typename Key, typename Value>
int ShmHashMap<Key, Value>::AddNodeItem(int index, const Key &key,
                                        const Value &value, int expire) {
  void *ptr = (Item *)Allocate();

  if (ptr == NULL) return RET_NO_MEMORY;

  // construct node data
  Item *new_node = new (ptr) Item;
  new_node->_invalid.store(0, std::memory_order_release);
  new_node->_key = key;
  new_node->_value = value;
  new_node->_next = OFFSET_NULL;
  new_node->_expire = expire != 0 ? time(NULL) + expire : 0;
  new_node->_del_next = OFFSET_NULL;

  // exchange tail
  BucketItem &bucket = _buckets[index];
  uint64_t old_offset =
      bucket._tail.exchange(NodeToOffset(new_node), std::memory_order_acq_rel);
  Item *old_node = OffsetToNode(old_offset);
  // int count = bucket._count.load(std::memory_order_acquire);
  if (old_node == NULL) {
    // empty list
    bucket._head = NodeToOffset(new_node);
  } else {
    old_node->_next = NodeToOffset(new_node);
  }

  bucket._count.fetch_add(1, std::memory_order_acq_rel);

  return RET_OK;
}

template <typename Key, typename Value>
Item *ShmHashMap<Key, Value>::GetNode(uint32_t index, const Key &key) {
  BucketItem &bucket = _buckets[index];
  Item *p = OffsetToNode(bucket._head);

  while (p != NULL) {
    if (p->_key == key) return p;
    p = OffsetToNode(p->_next);
  }

  return NULL;
};

template <typename Key, typename Value>
void *ShmHashMap<Key, Value>::Allocate() {
  return _pool->Allocate();
};

template <typename Key, typename Value>
void ShmHashMap<Key, Value>::Free(Item *ptr) {
  _pool->Free(ptr);
};
#undef Item
}  // namespace ShmMap

#endif  // SHM_MAP_H