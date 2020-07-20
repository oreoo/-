#include "util/file.h"
#include "param.h"


Param::Param(const char *path) {
    FILE *file = openfile(path, "r");
    char line[256], k[64], v[64];
    while (fgets(line, sizeof(line), file)) {
        sscanf(line, "%[^=]=%[^;]", k, v);
        (*this)[k] = v;
    }
    fclose(file);
}


int Param::print(const char *fmt) {
    for (const auto &i : *this) {
        printf(fmt, i.first.c_str(), i.second.c_str());
    }
    return 0;
}
