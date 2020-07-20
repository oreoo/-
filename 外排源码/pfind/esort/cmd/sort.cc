#include <cstdint>
#include <cstdio>
#include <ctime>

#include "esort/float.h"
#include "esort/sort.h"
#include "util/param.h"
#include "util/ui.h"


int main(int argc, const char **argv) {
    time_t time_start, time_end;
    time(&time_start);
    printf("start at: %s", ctime(&time_start));

    if (argc > 2) {
        fprintf(stderr, "too many arguments, only path to config required.\n");
        exit(-1);
    }
    const char *param_path = (argc == 2) ? argv[1] : "sort.param";
    printf("parsing config \"%s\"\n", param_path);
    Param param(param_path);
    param.print();
    const char keys[][16] = {"path_input", "path_output", "batch_size", "num_way", "num_thread"};
    bool valid_param = true;
    for (auto key : keys) {
        if (!param.count(key)) {
            printf("\"%s\" is undefined, please check \"%s\".\n", key, param_path);
            valid_param = false;
        }
    }
    if (!valid_param) {
        exit(-1);
    }
    const char *path_input = param["path_input"].c_str();
    const char *path_output = param["path_output"].c_str();
    size_t batch_size = strtol(param["batch_size"].c_str(), nullptr, 10);
    size_t n_way = strtol(param["num_way"].c_str(), nullptr, 10);
    size_t n_job = strtol(param["num_thread"].c_str(), nullptr, 10);

    external_sort<uint64_t, uint64_t(-1), floats_encode, floats_decode, radix_sort_uint64>(
            path_output, path_input, batch_size, n_way, n_job, false);

    time(&time_end);
    printf("end at: %s", ctime(&time_end));
    printf("elapsed time: %ld sec\n", (time_end - time_start));
    return 0;
}
