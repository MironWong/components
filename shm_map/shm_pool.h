#ifndef SHM_POOL_H
#define SHM_POOL_H

#include <assert.h>

#include <any>
#include <atomic>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/offset_ptr.hpp>
#include <boost/unordered_set.hpp>
#include <string>
#include <vector>

namespace ShmPool {

template <typename Obj>
struct MemoryNode {
  bool _used;
  Obj _data;
};

#define Node MemoryNode<Obj>
#define NodeSize sizeof(Node)
#define OFFSET_NULL 1

const std::string QUEUE = "_queue";
using namespace boost::interprocess;

struct MemoryMeta {
  MemoryMeta(uint32_t node_size, uint32_t obj_size) {
    _obj_size = obj_size;
    _node_size = node_size;
    _data = 0;

    _free_list_head = OFFSET_NULL;
    _free_list_tail = OFFSET_NULL;
  }

  uint32_t _obj_size;
  uint32_t _node_size;
  offset_ptr<void> _data;

  std::atomic<uint64_t> _free_list_head;
  std::atomic<uint64_t> _free_list_tail;

  std::atomic<uint64_t> _write_index;
  std::atomic<uint64_t> _read_index;
};

template <typename Obj>
class MemoryPool {
 public:
  MemoryPool() {}
  MemoryPool(std::string name, uint32_t node_size,
             managed_shared_memory *segment) {
    assert(segment != NULL);

    // two nodes reserved
    node_size += 2;

    _node_size = node_size;
    _segment = segment;
    _meta = _segment->find_or_construct<MemoryMeta>(name.c_str())(node_size,
                                                                  NodeSize);

    assert(_meta->_node_size == node_size);
    assert(_meta->_obj_size == NodeSize);

    _free_queue = _segment->find_or_construct<uint64_t>(
        (name + QUEUE).c_str())[node_size](OFFSET_NULL);
    if (_meta->_data == 0) {
      _meta->_data = _segment->allocate(node_size * NodeSize);
      _data = _meta->_data.get();

      // initialize
      memset(_data, 0, node_size * NodeSize);

      for (int i = 0; i < node_size; ++i) {
        Node *node = (Node *)(_data + NodeSize * i);
        node->_used = false;

        _free_queue[i] = (NodeSize * i);
      }

      _meta->_read_index = 0;
      _meta->_write_index = 0;
    } else {
      _data = _meta->_data.get();
    }

    _write_index_ptr = &_meta->_write_index;
    _read_index_ptr = &_meta->_read_index;
  }

  Obj *Allocate() {
    // TIP: there is risk uint64 overflow causing index error,
    // however it takes nearly 2W years if allocating at 3kw/qps
    uint64_t index =
        _read_index_ptr->fetch_add(1, std::memory_order_acq_rel) % _node_size;
    if (_free_queue[index] == OFFSET_NULL) return NULL;

    Node *node = GetNodeByOffset(_free_queue[index]);
    _free_queue[index] = OFFSET_NULL;

    node->_used = true;
    return &node->_data;
  }

  void Free(Obj *ptr) {
    // TODO: resume if crash when free
    Node *node = GetNodeByObj(ptr);
    if (node->_used == false) return;

    node->_used = false;
    uint64_t node_offset = GetOffsetByNode(node);
    int index =
        _write_index_ptr->fetch_add(1, std::memory_order_acq_rel) % _node_size;
    // check double free
    assert(_write_index_ptr->load(std::memory_order_consume) <=
           _read_index_ptr->load(std::memory_order_consume));
    _free_queue[index] = node_offset;
  }

  Obj *GetObjByOffset(uint64_t offset) {
    if (__builtin_expect(
            !GetNodeByOffset(offset)->_used || offset == OFFSET_NULL, 1))
      return NULL;
    else
      return (Obj *)(_data + offset);
  }

  uint64_t GetOffsetByObj(Obj *ptr) { return (char *)ptr - (char *)_data; }

  // only check for restart
  void SyncMemory(boost::unordered_set<Obj *> &obj_set) {
    void *check_ptr = _data;

    boost::unordered_set<Node *> free_set;
    uint64_t index = _read_index_ptr->load(std::memory_order_consume),
             count = 0;
    while (_free_queue[index % _node_size] != OFFSET_NULL &&
           count++ < _node_size) {
      free_set.insert(GetNodeByOffset(_free_queue[index++ % _node_size]));
    }

    for (int i = 0; i < _node_size; ++i) {
      Node *node = (Node *)(_data + NodeSize * i);
      if (node->_used) {
        if (obj_set.find(&node->_data) == obj_set.end()) {
          Free(&node->_data);
        }
      } else {
        if (free_set.find(node) == free_set.end()) {
          Free(&node->_data);
        }
      }
    }
  }

 private:
  Node *GetNodeByOffset(uint64_t offset) {
    offset = (offset / NodeSize) * NodeSize;
    return (Node *)(_data + offset);
  }

  Node *GetNodeByObj(Obj *ptr) {
    return GetNodeByOffset((((char *)ptr - (char *)_data) / NodeSize) *
                           NodeSize);
  }

  uint64_t GetOffsetByNode(Node *ptr) { return (char *)ptr - (char *)_data; }

  Node *Next(uint64_t offset) {
    Node *current = GetNodeByOffset(offset);
    return GetNodeByOffset(current->_next);
  }

  MemoryMeta *_meta;
  managed_shared_memory *_segment;

  void *_data;

  uint32_t _node_size;

  uint64_t *_free_queue;
  std::atomic<uint64_t> *_write_index_ptr;
  std::atomic<uint64_t> *_read_index_ptr;
};

#undef Node
}  // namespace ShmPool
#endif  // SHM_POOL_H