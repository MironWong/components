
cc_library(
  name = 'shm_pool',
  hdrs = [
    'shm_pool.h',
  ],
  deps = [
    '//thirdparty/boost:boost',
  ],
  extra_cppflags = [
    '-Wno-pointer-arith',
  ],
)

cc_binary(
  name = 'shm_pool_test',
  srcs = [
    'shm_pool_test.cc',
  ],
  deps = [
    ':shm_pool',
    '//thirdparty/boost:boost',
  ],
  extra_cppflags = [
    '-Wno-pointer-arith',
  ],
)

cc_library(
  name = 'shm_map',
  hdrs = [
    'shm_map.h',
  ],
  deps = [
    ':shm_pool',
    '//thirdparty/boost:boost',
  ],
  extra_cppflags = [
    '-Wno-pointer-arith',
  ],
)

cc_binary(
  name = 'shm_map_test',
  srcs = [
    'shm_map_test.cc',
  ],
  deps = [
    ':shm_map',
    ':shm_pool',
    '//thirdparty/boost:boost',
  ],
  extra_cppflags = [
    '-Wno-pointer-arith',
    '-Werror=unused-variable',
    '-Wno-ignored-qualifiers',
  ],
)
