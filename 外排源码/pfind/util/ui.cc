#include <cmath>
#include <cstdio>
#include <regex>

#include "ui.h"


ProgressBar::ProgressBar(size_t n, const char *fmt) : i_(0), n_(n), fmt_(std::string(fmt)) {
    fmt_ = std::regex_replace(fmt_, std::regex("%-?\\d*d"), "%" + std::to_string(int(log10(n)) + 1) + "zu");
    std::smatch results;
    std::regex_search(fmt_, results, std::regex("%-?\\d+s"));
    sscanf(results[0].str().c_str(), "%%%zus", &width_);
}


size_t ProgressBar::update() {
    time(&time_end_);
    auto buf = new char[width_ + 1];
    for (size_t i = 0; i < i_ * width_ / n_; ++i) {
        buf[i] = '=';
    }
    for (size_t i = i_ * width_ / n_; i < width_; ++i) {
        buf[i] = '-';
    }
    buf[width_] = 0;
    printf("%s", prefix.c_str());
    printf(fmt_.c_str(), buf, i_, n_);
    if (timer) {
        printf("  [%ld sec]", time_end_ - time_start_);
    }
    printf("\r");
    fflush(stdout);
    delete[] buf;
    return i_;
}


size_t ProgressBar::start(bool print) {
    time(&time_start_);
    return print ? update() : i_;
}


size_t ProgressBar::end(bool print) {
    if (print) {
        update();
    }
    printf("\n");
    return i_;
}


size_t ProgressBar::set(size_t i, bool print) {
    i_ = i;
    return print ? update() : i_;
}


size_t ProgressBar::increase(size_t i, bool print) {
    i_ += i;
    return print ? update() : i_;
}
