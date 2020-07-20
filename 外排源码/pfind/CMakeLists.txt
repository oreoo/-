if (WIN32)
    set(CMAKE_EXE_LINKER_FLAGS "-static")
endif ()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -O2")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O2")

find_package(Threads)

add_subdirectory(esort)
