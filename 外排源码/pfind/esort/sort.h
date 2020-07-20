#ifndef PFIND_ESORT_SORT_H_
#define PFIND_ESORT_SORT_H_

#include <cstdio>
#include <algorithm>
#include <array>
#include <mutex>
#include <thread>
#include <vector>

#include "util/file.h"
#include "util/ui.h"


template<typename T>
using Encoder = size_t (*)(T *dst, int n, const char *src, char **end_ptr, size_t *invalid);

template<typename T>
using Decoder = char *(*)(char *dst, int n, const T *src);

template<typename T>
using ISorter = void (*)(T *first, T *last);


void radix_sort_uint64(uint64_t *first, uint64_t *last) {
    union uint64_t_union {
        uint64_t n;
        struct {
            uint8_t r0, r1, r2, r3, r4, r5, r6, r7;
        };
    };
    auto first_ = (uint64_t_union *) first;
    auto last_ = (uint64_t_union *) last;
    std::vector<std::vector<uint64_t_union>> buckets0(1u << 8u);
    std::vector<std::vector<uint64_t_union>> buckets1(1u << 8u);
    std::for_each(buckets0.begin(), buckets0.end(), [](auto &bucket) { bucket.reserve(1u << 10u); });
    std::for_each(buckets1.begin(), buckets1.end(), [](auto &bucket) { bucket.reserve(1u << 10u); });
    std::for_each(first_, last_, [&buckets0](auto item) { buckets0[item.r0].push_back(item); });
    for (auto &bucket : buckets0) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets1](auto item) { buckets1[item.r1].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets1) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets0](auto item) { buckets0[item.r2].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets0) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets1](auto item) { buckets1[item.r3].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets1) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets0](auto item) { buckets0[item.r4].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets0) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets1](auto item) { buckets1[item.r5].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets1) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets0](auto item) { buckets0[item.r6].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets0) {
        std::for_each(bucket.begin(), bucket.end(), [&buckets1](auto item) { buckets1[item.r7].push_back(item); });
        bucket.clear();
    }
    for (auto &bucket : buckets1) {
        memcpy(first_, bucket.data(), bucket.size() * sizeof(uint64_t));
        first_ += bucket.size();
    }
}


template<typename T, T T_MAX, Encoder<T> encode, ISorter<T> sort, size_t BUFSZ = 16u << 20u>
void job_divide(const char *fmt_dst, FILE *src, size_t batch_size, size_t *n_batch, size_t *n_valid, size_t *n_invalid,
                ProgressBar &bar, std::mutex &mtx_src, std::mutex &mtx_stat) {
    auto buf_T = new T[batch_size + 1];  // 1: for T_MAX
    auto buf_str = new char[BUFSZ + 256];

    bool empty = true;
    char *end_ptr = nullptr;
    while (true) {
        size_t i = 0, n_invalid_ = 0;
        while (i < batch_size) {
            if (empty) {
                mtx_src.lock();
                if (!readlines(buf_str, BUFSZ, 256, src)) {
                    mtx_src.unlock();
                    break;
                }
                mtx_src.unlock();
            }
            i += encode(buf_T + i, batch_size - i, (empty ? buf_str : end_ptr + 1), &end_ptr, &n_invalid_);
            empty = (*end_ptr == 0);
        }
        if (i == 0) {
            break;
        }
        sort(buf_T, buf_T + i);
        buf_T[i] = T_MAX;

        mtx_stat.lock();
        char path_dst[256];
        sprintf(path_dst, fmt_dst, (*n_batch)++);
        *n_valid += i;
        *n_invalid += n_invalid_;
        bar.set(getfilepos(src) >> 20u);
        mtx_stat.unlock();

        FILE *file_dst = openfile(path_dst, "wb");
        fwrite(buf_T, sizeof(T), i + 1, file_dst);
        fclose(file_dst);
    }

    delete[] buf_T;
    delete[] buf_str;
}


template<typename T, T T_MAX, Encoder<T> encode, ISorter<T> sort>
size_t divide(const char *fmt_dst, const char *src, size_t batch_size, size_t n_job) {
    auto size_file = (getfilesize(src) + (1u << 20u) - 1) >> 20u;
    auto bar = ProgressBar(size_file, "divide: [%64s]  %d MiB / %d MiB");
    bar.timer = true;
    bar.start();

    FILE *file_src = openfile(src, "r");
    size_t n_batch = 0, n_valid = 0, n_invalid = 0;
    std::mutex mtx_src, mtx_stat;
    std::vector<std::thread> workers;
    for (int i = 0; i < n_job; ++i) {
        workers.push_back(std::thread(job_divide<T, T_MAX, encode, sort>,
                                      fmt_dst, file_src, batch_size, &n_batch, &n_valid, &n_invalid,
                                      std::ref(bar), std::ref(mtx_src), std::ref(mtx_stat)));
    }
    std::for_each(workers.begin(), workers.end(), [](auto &worker) { worker.join(); });

    fclose(file_src);
    bar.set(size_file);
    bar.end();
    printf("dataset size: %zu\ninvalid item: %zu\n", n_valid, n_invalid);
    return n_batch;
}


template<typename T, Decoder<T> decode, size_t RBUFSZ = 1u << 16u, size_t WBUFSZ = 16u << 20u, size_t STRLEN = 18>
int job_merge(FILE *dst, const std::vector<FILE *> &srcs, size_t n, size_t n_job, ProgressBar &bar, bool bin = true) {
    auto rbuf = new T[srcs.size()][RBUFSZ];
    auto wbuf_T = new T[WBUFSZ];
    auto wbuf_str = !bin ? new char[STRLEN * WBUFSZ] : nullptr;

    auto tree = new std::pair<size_t, T>[srcs.size() * 2];
    auto reader = new T *[srcs.size() * 2];  // to align
    auto reader_end = new T *[srcs.size() * 2];  // to align
    auto srcs_ = &(srcs[0]) - srcs.size();  // to align

    for (size_t i = 0; i < srcs.size(); ++i) {  // init leaf
        tree[i + srcs.size()].first = i + srcs.size();
        fread(&(tree[i + srcs.size()].second), sizeof(T), 1, srcs[i]);
        reader[i + srcs.size()] = rbuf[i] + RBUFSZ;  // to the end
        reader_end[i + srcs.size()] = rbuf[i] + RBUFSZ;  // to the end
    }
    for (size_t i = srcs.size() - 1; i; --i) {  // build tree
        tree[i] = tree[i * 2 + (tree[i * 2].second > tree[i * 2 + 1].second)];
    }

    while (true) {
        size_t n_ = (n >= WBUFSZ ? WBUFSZ : n);
        n -= n_;
        if (!n_) {
            break;
        }
        size_t k;
        for (size_t i = 0; i < n_; ++i) {
            wbuf_T[i] = tree[1].second;
            if (reader[tree[1].first] == reader_end[tree[1].first]) {  // the buffer is empty
                int n_read = fread(reader[tree[1].first] - RBUFSZ, sizeof(T), RBUFSZ, srcs_[tree[1].first]);
                if (n_read != RBUFSZ) {
                    memcpy(reader[tree[1].first] - n_read, reader[tree[1].first] - RBUFSZ, sizeof(T) * n_read);
                }
                reader[tree[1].first] -= n_read;
            }
            tree[tree[1].first].second = *(reader[tree[1].first]++);
            k = tree[1].first;
            while (k != 1) {
                tree[k >> 1u] = tree[k ^ (tree[k].second >= tree[k ^ 1u].second)];
                k >>= 1u;
            }
        }
        if (bin) {
            fwrite(wbuf_T, sizeof(T), n_, dst);
        } else {
            std::vector<std::pair<char *, char *>> wbufs(n_job);
            std::vector<std::thread> workers;
            size_t n_each = n_ / n_job + 1;
            for (int i = 0; i < n_job; ++i) {
                size_t n_begin = n_each * i;
                wbufs[i].first = wbuf_str + n_begin * STRLEN;
                workers.push_back(std::thread(
                        [](auto dst, auto n, auto src, auto end_ptr) { *end_ptr = decode(dst, n, src); },
                        wbufs[i].first, std::min(n_ - n_begin, n_each), wbuf_T + n_begin, &(wbufs[i].second)));
            }
            for (int i = 0; i < n_job; ++i) {
                workers[i].join();
                fwrite(wbufs[i].first, sizeof(char), wbufs[i].second - wbufs[i].first, dst);
            }
        }
        bar.increase(n_ >> 20u);
    }

    delete[] rbuf;
    delete[] wbuf_T;
    delete[] wbuf_str;
    delete[] tree;
    delete[] reader;
    delete[] reader_end;
    return 0;
}


template<typename T, Decoder<T> decode>
const char *merge(const char *dst, const char *fmt, size_t n_src, size_t n_way, size_t n_job, bool bin = true) {
    size_t iter = 0;
    char fmt_dst[256], fmt_src[256], src_path[256], dst_path[256];
    do {
        size_t n_batch = (n_src + n_way - 1) / n_way;
        sprintf(fmt_src, fmt, iter++);
        sprintf(fmt_dst, fmt, iter);

        auto size_srcs = new uint64_t[n_src + 1];
        size_srcs[0] = 0;
        for (size_t i = 1; i <= n_src; ++i) {
            sprintf(src_path, fmt_src, i - 1);
            // handle T_MAX
            size_srcs[i] = getfilesize(src_path) / sizeof(T) + size_srcs[i - 1] - ((i % n_way) && (i != n_src));
        }
        for (size_t i = 1; i <= n_src; ++i) {
            size_srcs[i] = (size_srcs[i] + (1u << 20u) - 1) >> 20u;  // M, ceil
        }
        char fmt_bar[64];
        sprintf(fmt_bar, "iter %zu: [%%64s]  %%d M / %%d M  (%zu -> %zu)", iter, n_src, n_batch);
        auto bar = ProgressBar(size_srcs[n_src], fmt_bar);
        bar.timer = true;
        bar.start();

        bool bin_ = bin || n_src > n_way;  // for this iter
        for (size_t i_batch = 0; i_batch < n_batch; ++i_batch) {
            sprintf(dst_path, fmt_dst, i_batch);
            FILE *file_dst = openfile(dst_path, bin_ ? "wb" : "w");
            size_t n_way_batch = ((i_batch + 1) != n_batch) ? n_way : (n_src - i_batch * n_way);
            std::vector<FILE *> file_srcs;
            int n = 0;
            for (size_t i_way = 0; i_way < n_way_batch; ++i_way) {  // open srcs, calc the size
                sprintf(src_path, fmt_src, i_batch * n_way + i_way);
                file_srcs.push_back(openfile(src_path, "rb"));
                n += getfilesize(src_path);
            }
            n = n / sizeof(T) - n_way_batch + bin_;
            job_merge<T, decode>(file_dst, file_srcs, n, n_job, bar, bin_);
            std::for_each(file_srcs.begin(), file_srcs.end(), fclose);
            fclose(file_dst);
            for (size_t i_way = 0; i_way < n_way_batch; ++i_way) {
                sprintf(src_path, fmt_src, i_batch * n_way + i_way);
                remove(src_path);
            }
            bar.set(size_srcs[i_batch * n_way + n_way_batch], false);
        }
        bar.end();
        n_src = n_batch;
        delete[] size_srcs;
    } while (n_src > 1);
    rename(dst_path, dst);
    return dst;
}


template<typename T, T T_MAX, Encoder<T> encode, Decoder<T> decode, ISorter<T> sort = std::sort>
int external_sort(const char *dst, const char *src, size_t batch_size, size_t n_way, size_t n_job, bool bin = true) {
    const char tmp[] = ".tmp/";
    bool is_created = false;
    if (!isdir(tmp)) {
        makedirs(tmp);
        is_created = true;
    }

    char fmt[64];
    sprintf(fmt, "%s%%04zx_%%%%04zx.data", tmp);  // .tmp~/%04zx_%%04zx.data
    char fmt_divide_dst[64];
    sprintf(fmt_divide_dst, fmt, 0);

    size_t n_src = divide<T, T_MAX, encode, sort>(fmt_divide_dst, src, batch_size, n_job);
    merge<T, decode>(dst, fmt, n_src, n_way, n_job, bin);

    if (is_created) {
        remove(tmp);
    }
    return 0;
}


#endif  // PFIND_ESORT_SORT_H_
