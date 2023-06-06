#include "./shm_pool.h"

#include <boost/interprocess/managed_shared_memory.hpp>
#include <iostream>

struct ListNode {
  int value;
  uint64_t next;
};

using namespace boost::interprocess;
using namespace ShmPool;

int main(int argc, char *argv[]) {
  if (argc == 1) {
    managed_shared_memory segment(create_only, "MySharedMemory",
                                  10 * 1024 * 1024);
    MemoryPool<ListNode> region("list_node", 20, &segment);

    ListNode *head = region.Allocate();
    head->value = 0;
    head->next = 0;

    ListNode *current = head;
    for (int i = 1; i < 10; ++i) {
      ListNode *node = region.Allocate();
      node->value = i;
      node->next = OFFSET_NULL;
      current->next = region.GetOffsetByObj(node);

      current = node;
    }

    current = head;
    while (current != NULL) {
      std::cout << current->value << std::endl;
      current = region.GetObjByOffset(current->next);
    }

    uint64_t head_offset = region.GetOffsetByObj(head);
    segment.construct<uint64_t>("head")(head_offset);
  } else if (argc == 2) {
    managed_shared_memory segment(open_only, "MySharedMemory");
    uint64_t *head_offset = segment.find<uint64_t>("head").first;

    std::cout << head_offset << std::endl;
    MemoryPool<ListNode> region("list_node", 20, &segment);

    ListNode *current = region.GetObjByOffset(*head_offset);
    while (current != NULL) {
      std::cout << current->value << std::endl;
      current = region.GetObjByOffset(current->next);
    }
  } else {
    shared_memory_object::remove("MySharedMemory");
    std::cout << "remove shm" << std::endl;
  }
  return 0;
}