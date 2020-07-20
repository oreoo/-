#ifndef PFIND_UTIL_PARAM_H_
#define PFIND_UTIL_PARAM_H_

#include <map>
#include <string>


class Param : public std::map<std::string, std::string> {
public:
    explicit Param(const char *path);

    ~Param() = default;

    int print(const char *fmt = "config: %s=%s\n");
};


#endif  // PFIND_UTIL_PARAM_H_
