#ifndef SIN_MAP_H
#define SIN_MAP_H

#include <stdio.h>
#include <time.h>
#include <functional>

#include <atomic>
#include <vector>

namespace SinMap {
template <typename Key, typename Value>
struct ItemNode {
  ItemNode *_next;
  Key _key;
  Value _value;
  volatile int _expire;

  std::atomic<int> _invalid;  // 0 - valid  1 - add garbage list  2 - should to
                              // delete 3 - writing
  ItemNode *_del_next;
};

struct BucketItem {
  std::atomic<int> _count;

  void *_head;
  std::atomic<void *> _tail;

  BucketItem() {
    _count = 0;
    _head = NULL;
    _tail = NULL;
  }
};

enum SinHashRet {
  RET_OK = 0,
  RET_NOT_FOUND = 1,
};

enum ItemStatus {
  VALID = 0,
  COLLECTING = 1,
  WAITING_DELETE = 2,
  WRITING = 3,
  READING = 4,
};

#define Item ItemNode<Key, Value>

template <typename Key, typename Value>
class SinHashMap {
 public:
  explicit SinHashMap(int bucket_size);

  virtual ~SinHashMap();

  void Insert(const Key &key, const Value &value, int expire = 0);

  int Get(const Key &key, Value &value);

  int GetAllValues(std::vector<Value> &values);

  int GetCount();

  void GC();

 protected:
  virtual void *Allocate(int size) = 0;

  virtual void Free(void *ptr) = 0;

  virtual uint32_t HashCode(const Key &key) = 0;

 private:
  void SafeFree();

  void Scan();

  void AddGarbageList(Item *node);

  void RemoveExpireNode(Item *p, BucketItem &bucket);

  void AddNodeItem(int index, const Key &key, const Value &value, int expire);

  Item *GetNode(uint32_t index, const Key &key);

  BucketItem *_buckets;
  int _bucket_size;
  std::atomic<uint32_t> _gc_timestamp;

  // garbage list
  Item *_garbage_list_head;
  Item *_garbage_list_tail;
};

// implements
template <typename Key, typename Value>
SinHashMap<Key, Value>::SinHashMap(int bucket_size) {
  if (bucket_size <= 0) bucket_size = 1024;
  _buckets = new BucketItem[bucket_size];
  _bucket_size = bucket_size;
  _garbage_list_head = NULL;
  _garbage_list_tail = NULL;
}

template <typename Key, typename Value>
SinHashMap<Key, Value>::~SinHashMap() {
  if (_buckets != NULL) delete _buckets;
  _buckets = NULL;
}

template <typename Key, typename Value>
void SinHashMap<Key, Value>::Insert(const Key &key, const Value &value,
                                    int expire) {
  int index = HashCode(key) % _bucket_size;

  Item *item = GetNode(index, key);

  if (item == NULL || (item->_expire != 0 && item->_expire < time(NULL))) {
    AddNodeItem(index, key, value, expire);
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
        AddNodeItem(index, key, value, expire);
        return;
      }
    }

    item->_value = value;
    item->_expire = expire != 0 ? time(NULL) + expire : 0;
    item->_invalid.store(VALID, std::memory_order_release);
  }
}

template <typename Key, typename Value>
int SinHashMap<Key, Value>::Get(const Key &key, Value &value) {
  int index = HashCode(key) % _bucket_size;
  Item *item = GetNode(index, key);

  if (item == NULL || (item->_expire != 0 && item->_expire < time(NULL))) {
    return RET_NOT_FOUND;
  }

  // lock the item
  // TODO: use reference count to allow multiple readers read at same time
  int invalid = VALID, read = READING;
  while (!item->_invalid.compare_exchange_strong(invalid, read,
                                                 std::memory_order_acq_rel)) {
  }

  value = item->_value;
  item->_invalid.store(VALID, std::memory_order_release);

  return RET_OK;
}

template <typename Key, typename Value>
int SinHashMap<Key, Value>::GetAllValues(std::vector<Value> &values) {
  for (int i = 0; i < _bucket_size; ++i) {
    BucketItem &bucket = _buckets[i];
    Item *p = (Item *)bucket._head;

    while (p != NULL) {
      values.push_back(p->_value);
      p = p->_next;
    }
  }
  return 0;
}

template <typename Key, typename Value>
int SinHashMap<Key, Value>::GetCount() {
  int sum = 0;
  for (int i = 0; i < _bucket_size; ++i)
    sum += _buckets[i]._count.load(std::memory_order_consume);
  return sum;
}

template <typename Key, typename Value>
void SinHashMap<Key, Value>::GC() {
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
void SinHashMap<Key, Value>::SafeFree() {
  Item *p0 = _garbage_list_head;
  if (p0 == NULL) {
    return;
  }

  Item *p1 = p0->_del_next;

  while (p1) {
    if (p1->_invalid.fetch_add(1, std::memory_order_acq_rel) == 2) {
      // free
      p0->_del_next = p1->_del_next;
      p1->~Item();
      Free(p1);
      p1 = p0->_del_next;
    } else {
      p0 = p1;
      p1 = p1->_del_next;
    }
  }
  _garbage_list_tail = p0;
}

template <typename Key, typename Value>
void SinHashMap<Key, Value>::Scan() {
  for (int i = 0; i < _bucket_size; ++i) {
    BucketItem &bucket = _buckets[i];

    Item *p0 = (Item *)bucket._head;

    if (p0 == NULL) continue;

    Item *p1 = p0->_next;

    while (p1 != NULL && p1->_next != NULL) {
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
          p1 = p0->_next;
        }

      } else {
        p0 = p1;
        p1 = p1->_next;
      }
    }

    // check head invalid
    p0 = (Item *)bucket._head;
    if (p0->_expire && p0->_expire < time(NULL)) {
      int valid = VALID;
      int collecting = COLLECTING;

      // lock the head
      if (p0->_invalid.compare_exchange_strong(valid, collecting,
                                               std::memory_order_acq_rel)) {
        RemoveExpireNode(p0, bucket);
        if (p0 == bucket._tail.load(std::memory_order_consume)) {
          void *expected = p0, *desire = NULL;
          if (bucket._tail.compare_exchange_strong(expected, desire,
                                                   std::memory_order_acq_rel)) {
            bucket._head = NULL;
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
void SinHashMap<Key, Value>::AddGarbageList(Item *node) {
  if (_garbage_list_head == NULL) {
    _garbage_list_head = node;
    _garbage_list_tail = node;
  } else {
    _garbage_list_tail->_del_next = node;
    _garbage_list_tail = node;
  }
}

template <typename Key, typename Value>
void SinHashMap<Key, Value>::RemoveExpireNode(Item *p, BucketItem &bucket) {
  //    // (1) atomic change invalid to 1
  //    p->_invalid.store(1, std::memory_order_release);

  // (2) add garbage list
  AddGarbageList(p);

  // (3) count reduce 1
  bucket._count.fetch_sub(1, std::memory_order_acq_rel);
}

template <typename Key, typename Value>
void SinHashMap<Key, Value>::AddNodeItem(int index, const Key &key,
                                         const Value &value, int expire) {
  void *ptr = (Item *)Allocate(sizeof(Item));

  // construct node data
  Item *new_node = new (ptr) Item;
  new_node->_invalid.store(0, std::memory_order_release);
  new_node->_key = key;
  new_node->_value = value;
  new_node->_next = NULL;
  new_node->_expire = expire != 0 ? time(NULL) + expire : 0;
  new_node->_del_next = NULL;

  // exchange tail
  BucketItem &bucket = _buckets[index];
  Item *old_node =
      (Item *)bucket._tail.exchange(new_node, std::memory_order_acq_rel);

  if (old_node == NULL) {
    // empty list
    bucket._head = new_node;
  } else {
    old_node->_next = new_node;
  }

  bucket._count.fetch_add(1, std::memory_order_acq_rel);
}

template <typename Key, typename Value>
Item *SinHashMap<Key, Value>::GetNode(uint32_t index, const Key &key) {
  BucketItem &bucket = _buckets[index];
  Item *p = (Item *)bucket._head;

  while (p != NULL) {
    if (p->_key == key) return p;
    p = p->_next;
  }

  return NULL;
};
#undef Item
}  // namespace SinMap

#endif  // SIN_MAP_H