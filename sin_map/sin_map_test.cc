#include <assert.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>

#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "./sin_map.h"

using namespace std;
using namespace SinMap;

int READ_AND_WRITE_NUM = 100000;

struct NsCalcTool {
  std::atomic<uint64_t> _count;
  std::atomic<uint64_t> _time;

  // just test for performance, don't care the thread safe problem
  uint64_t AverageCost() {
    if (_count.load(std::memory_order_consume) == 0) return 0;
    uint64_t average = _time.load(std::memory_order_consume) /
                       _count.load(std::memory_order_consume);
    return average;
  }

  void Calc(struct timespec& begin_, struct timespec& end_) {
    uint64_t cost = end_.tv_sec * 1000000000 + end_.tv_nsec -
                    begin_.tv_sec * 1000000000 - begin_.tv_nsec;
    _count.fetch_add(1, std::memory_order_acq_rel);
    _time.fetch_add(cost, std::memory_order_acq_rel);
  }
};

#ifdef STRING_TEST
class MyHashMap : public SinHashMap<string, string> {
 public:
  MyHashMap(int size) : SinHashMap<string, string>(size) {}

 protected:
  virtual void* Allocate(int size) { return malloc(size); }

  virtual void Free(void* ptr) { free(ptr); }

  virtual uint32_t HashCode(const string& key) {
    return std::hash<string>{}(key);
  }
};
#else

class MyHashMap : public SinHashMap<uint32_t, uint32_t> {
 public:
  MyHashMap(int size) : SinHashMap<uint32_t, uint32_t>(size) {
    _allocate_detector._time = 0;
    _allocate_detector._count = 0;

    _deallocate_detector._time = 0;
    _deallocate_detector._count = 0;
  }

  NsCalcTool _allocate_detector;
  NsCalcTool _deallocate_detector;

 protected:
  virtual void* Allocate(int size) {
    void* ptr = malloc(size);

    return ptr;
  }

  virtual void Free(void* ptr) { free(ptr); }

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
  MyHashMap hash_map(2048);

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
  int INSERT_NUM = READ_AND_WRITE_NUM;

  for (int i = 0; i < INSERT_NUM; ++i) {
    uint32_t key = GetRandomByMs() % MAX_UIN;

    // test
    key = INSERT_NUM * index + i;

#ifdef STRING_TEST
    string sKey = to_string(key);
    hash_map.Insert(sKey, sKey, 3);
#else
    hash_map.Insert(key, key);
#endif
  }
}

void ReadThreads(MyHashMap& hash_map, int index) {
  int READ_NUM = READ_AND_WRITE_NUM;
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
    //        cout << hash_map.GetCount() << endl;

    sleep(1);
  }
}

void MultipleThreadsTest() {
  MyHashMap hash_map(100000);

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

  /*
  thread gc = std::thread(std::bind(&GCThread, std::ref(hash_map)));
  gc.detach();

  thread calc = std::thread(std::bind(&CalcThread, std::ref(hash_map)));
  calc.detach();
  */

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i].join();
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i].join();
  }
}

mutex _mutux;
pthread_rwlock_t rwlock = PTHREAD_RWLOCK_INITIALIZER;
pthread_mutex_t pthread_mutex;

void StdReadThreads(std::map<uint32_t, uint32_t>& hash_map) {
  int READ_NUM = READ_AND_WRITE_NUM;
  for (int i = 0; i < READ_NUM; ++i) {
    uint32_t random = GetRandomByMs() % MAX_UIN;
#ifdef RWLOCK
    pthread_rwlock_rdlock(&rwlock);
#else
    pthread_mutex_lock(&pthread_mutex);
#endif
    auto iter = hash_map.find(random);
    if (iter != hash_map.end()) assert(iter->first == iter->second);
#ifdef RWLOCK
    pthread_rwlock_unlock(&rwlock);
#else
    pthread_mutex_unlock(&pthread_mutex);
#endif
  }
}

void StdInsertThreads(std::map<uint32_t, uint32_t>& hash_map) {
  int INSERT_NUM = READ_AND_WRITE_NUM;

  for (int i = 0; i < INSERT_NUM; ++i) {
    uint32_t key = GetRandomByMs() % MAX_UIN;
#ifdef RWLOCK
    pthread_rwlock_wrlock(&rwlock);
#else
    pthread_mutex_lock(&pthread_mutex);
#endif
    hash_map.insert(make_pair(key, key));
#ifdef RWLOCK
    pthread_rwlock_unlock(&rwlock);
#else
    pthread_mutex_unlock(&pthread_mutex);
#endif
  }
}

void StdMultipleThreadsTest() {
  std::map<uint32_t, uint32_t> hash_map;

  int READ_THREAD = 20;
  int WRITE_THREAD = 20;

  std::thread read[READ_THREAD];
  std::thread write[WRITE_THREAD];

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i] = std::thread(std::bind(&StdReadThreads, std::ref(hash_map)));
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i] = std::thread(std::bind(&StdInsertThreads, std::ref(hash_map)));
  }

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i].join();
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i].join();
  }
}

void UnorderedReadThreads(std::unordered_map<uint32_t, uint32_t>& hash_map) {
  int READ_NUM = READ_AND_WRITE_NUM;
  for (int i = 0; i < READ_NUM; ++i) {
    uint32_t random = GetRandomByMs() % MAX_UIN;
#ifdef RWLOCK
    pthread_rwlock_rdlock(&rwlock);
#else
    pthread_mutex_lock(&pthread_mutex);
#endif
    auto iter = hash_map.find(random);
    if (iter != hash_map.end()) assert(iter->first == iter->second);
#ifdef RWLOCK
    pthread_rwlock_unlock(&rwlock);
#else
    pthread_mutex_unlock(&pthread_mutex);
#endif
  }
}

void UnorderedInsertThreads(std::unordered_map<uint32_t, uint32_t>& hash_map) {
  int INSERT_NUM = READ_AND_WRITE_NUM;

  for (int i = 0; i < INSERT_NUM; ++i) {
    uint32_t key = GetRandomByMs() % MAX_UIN;
#ifdef RWLOCK
    pthread_rwlock_wrlock(&rwlock);
#else
    pthread_mutex_lock(&pthread_mutex);
#endif
    hash_map.insert(make_pair(key, key));
#ifdef RWLOCK
    pthread_rwlock_unlock(&rwlock);
#else
    pthread_mutex_unlock(&pthread_mutex);
#endif
  }
}

void UnorderedMultipleThreadsTest() {
  std::unordered_map<uint32_t, uint32_t> hash_map;

  int READ_THREAD = 20;
  int WRITE_THREAD = 20;

  std::thread read[READ_THREAD];
  std::thread write[WRITE_THREAD];

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i] = std::thread(std::bind(&UnorderedReadThreads, std::ref(hash_map)));
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i] =
        std::thread(std::bind(&UnorderedInsertThreads, std::ref(hash_map)));
  }

  for (int i = 0; i < READ_THREAD; ++i) {
    read[i].join();
  }

  for (int i = 0; i < WRITE_THREAD; ++i) {
    write[i].join();
  }
}

int main() {
  cout << MAX_UIN << endl;

  int num = READ_AND_WRITE_NUM;
  for (int i = 1; i <= 5; ++i) {
    int begin = time(0);
    READ_AND_WRITE_NUM = num * i;
    cout << "RUN-" << i << " " << READ_AND_WRITE_NUM << "/thread" << endl;

    pthread_mutex_init(&pthread_mutex, NULL);
    MultipleThreadsTest();

    cout << "sin hash map cost: " << time(0) - begin << endl;

    begin = time(0);

    UnorderedMultipleThreadsTest();
    cout << "unordered map cost: " << time(0) - begin << endl;

    begin = time(0);

    StdMultipleThreadsTest();
    cout << "std map cost: " << time(0) - begin << endl << endl << endl;
  }

  return 0;
}