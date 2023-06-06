
cc_library(
  name = 'sin_map',
  hdrs = [
    'sin_map.h',
  ],
  deps = [
    '//thirdparty/boost:boost',
  ],
  extra_cppflags = [
    '-Wno-pointer-arith',
    '-Werror=parentheses',
    '-Werror=ignored-qualifiers',
  ],
)

cc_binary(
  name = 'sin_map_test',
  srcs = [
    'sin_map_test.cc',
  ],
  deps = [
    ':sin_map',
    '//thirdparty/boost:boost',
  ],
  extra_cppflags = [
    '-Wno-pointer-arith',
    '-Werror=parentheses',
    '-Werror=ignored-qualifiers',
    '-Wno-ignored-qualifiers',
  ],
)