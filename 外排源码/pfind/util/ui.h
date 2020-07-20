#ifndef PFIND_UTIL_UI_H_
#define PFIND_UTIL_UI_H_

#include <ctime>
#include <string>


class ProgressBar {
public:
    explicit ProgressBar(size_t n, const char *fmt = "[%64s]  %d/%d");

    ~ProgressBar() = default;

    size_t update();

    size_t start(bool print = true);

    size_t end(bool print = true);

    size_t set(size_t i = 0, bool print = true);

    size_t increase(size_t i = 1, bool print = true);

    std::string prefix;
    bool timer = false;

private:
    size_t i_ = 0;
    size_t n_;
    std::string fmt_;

    size_t width_;
    time_t time_start_;
    time_t time_end_;
};


#endif  // PFIND_UTIL_UI_H_
