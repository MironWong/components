#include "./shm_map.h"

#include <boost/interprocess/managed_shared_memory.hpp>
#include <iostream>
#include <random>
#include <thread>

using namespace std;
using namespace ShmMap;
using namespace ShmPool;

#ifdef STRING_TEST
class MyHashMap : public ShmHashMap<string, string> {
 public:
  MyHashMap(int size) : ShmHashMap<string, string>(size) {}
  virtual ~MyHashMap() = default;

 protected:
  virtual void* Allocate(int size) { return malloc(size); }

  virtual void Free(void* ptr) { free(ptr); }

  virtual uint32_t HashCode(const string& key) {
    return std::hash<string>{}(key);
  }
};
#else

class MyHashMap : public ShmHashMap<uint32_t, uint32_t> {
 public:
  MyHashMap(std::string name, MemoryPool<ItemNode<uint32_t, uint32_t> >* pool,
            managed_shared_memory* segment, uint32_t size)
      : ShmHashMap<uint32_t, uint32_t>(name, pool, segment, size) {}
  virtual ~MyHashMap() = default;

 protected:
  virtual uint32_t HashCode(const uint32_t& key) {
    return key + key % 100 + (key / 100) % 35;
  }
};
#endif

const uint32_t MAX_UIN = 0xffffffff;

const uint64_t GetTimestampMS() {
  return (std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()))
      .count();
}

const uint64_t GetTimestampNs() {
  struct timespec tn;
  clock_gettime(CLOCK_REALTIME, &tn);

  return tn.tv_nsec;
}

const uint64_t GetRandomByMs() {
  std::default_random_engine engine;
  engine.seed(GetTimestampNs());
  return engine();
}

void SimpleTest() {
  boost::interprocess::managed_shared_memory managedSharedMemory(
      open_or_create, "MySharedMap", 1024 * 1024 * 1024);

  MemoryPool<ItemNode<uint32_t, uint32_t> > pool("pool", 10000000,
                                                 &managedSharedMemory);

  MyHashMap hash_map("MultipleTest", &pool, &managedSharedMemory, 2048);

#ifdef STRING_TEST
  string key1 = "2333", key2 = "6666", key3 = "666";
  string value;
  int ret = 0;
#else
  uint32_t key1 = 2333, key2 = 6666, key3 = 666;
  uint32_t value = 0, ret = 0;
#endif

  hash_map.Insert(key1, key1);
  hash_map.Insert(key2, key2);

  ret = hash_map.Get(key1, value);
  cout << ret << " " << value << endl;

  ret = hash_map.Get(key2, value);
  cout << ret << " " << value << endl;

  ret = hash_map.Get(key3, value);
  cout << ret << " " << value << endl;

  cout << "count: " << hash_map.GetCount() << endl;
}

void InsertThreads(MyHashMap& hash_map, int index) {
  int INSERT_NUM = 1000000;

  for (int i = 0; i < INSERT_NUM; ++i) {
    uint32_t key = GetRandomByMs() % MAX_UIN;

    // test
    key = INSERT_NUM * index + i;

#ifdef STRING_TEST
    string sKey = to_string(key);
    hash_map.Insert(sKey, sKey, 3);
#else
    hash_map.Insert(key, key, 3);
#endif

    // cout << GetTimestampMS << " " << "Insert_" << index << " " << key <<
    // endl;
  }
}

void ReadThreads(MyHashMap& hash_map, int index) {
  int READ_NUM = 10000000;
  for (int i = 0; i < READ_NUM; ++i) {
    uint32_t random = GetRandomByMs() % MAX_UIN;

#ifdef STRING_TEST
    string key = to_string(random);
    string value;
#else
    uint32_t key = random, value = 0;
#endif
    int ret = hash_map.Get(key, value);
    if (ret == 0 && key != value) {
      cout << "ERROR" << endl;
      exit(0);
    }

    //        cout << GetTimestampMS << " " << "Get_" << index << " "
    //                   << ret << " " << key << " " << value <<  endl;
  }
}

void GCThread(MyHashMap& hash_map) {
  while (true) {
    hash_map.GC();
    sleep(1);
  }
}

void CalcThread(MyHashMap& hash_map) {
  while (true) {
    cout << hash_map.GetCount() << endl;
    sleep(1);
  }
}

void MultipleThreadsTest() {
  boost::interprocess::managed_shared_memory managedSharedMemory(
      open_or_create, "MySharedMap", 1024 * 1024 * 1024);

  MemoryPool<ItemNode<uint32_t, uint32_t> > pool("pool", 10000000,
                                                 &managedSharedMemory);

  MyHashMap hash_map("MultipleTest", &pool, &managedSharedMemory, 2048);

  int READ_THREAD = 20;
  int WRITE_THREAD = 20;

  std::thread read[READ_THREAD];
  std::thread write[WRITE_THREAD];

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i] = std::thread(std::bind(&ReadThreads, std::ref(hash_map), i));
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i] = std::thread(std::bind(&InsertThreads, std::ref(hash_map), i));
  }

  thread gc = std::thread(std::bind(&GCThread, std::ref(hash_map)));
  gc.detach();

  thread calc = std::thread(std::bind(&CalcThread, std::ref(hash_map)));
  calc.detach();

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i].join();
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i].join();
  }
  sleep(15);
}

int main() {
  cout << MAX_UIN << endl;

  MultipleThreadsTest();

  return 0;
}