aux_source_directory(. SRC)
aux_source_directory(../util SRC_UTIL)

add_executable(sort cmd/sort.cc ${SRC} ${SRC_UTIL})
target_link_libraries(sort ${CMAKE_THREAD_LIBS_INIT})
