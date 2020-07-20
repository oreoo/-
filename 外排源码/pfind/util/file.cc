#include <sys/stat.h>
#include <cstring>

#if defined(_WIN32)
#include <direct.h>
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "file.h"


int isdir(const char *path) {
#if defined(_WIN32)
    DWORD attr = GetFileAttributes(path);
    if (attr == INVALID_FILE_ATTRIBUTES) {
        return false;
    }
    return (attr & FILE_ATTRIBUTE_DIRECTORY);
#else
    struct stat st = {0};
    stat(path, &st);
    return S_ISDIR(st.st_mode);
#endif
}


int makedir(const char *path) {
#if defined(_WIN32)
    return _mkdir(path);
#else
    return mkdir(path, 0700);
#endif
}


int makedirs(const char *path) {
    auto path_ = new char[strlen(path) + 1];
    strcpy(path_, path);
    if (path_[strlen(path) - 1] != '/') {  // append a '/' at the end of the path
        path_[strlen(path)] = '/';
        path_[strlen(path) + 1] = 0;
    }
    for (int i = 0; path_[i]; ++i) {
        if (path_[i] == '/') {
            path_[i] = 0;
            makedir(path_);
            path_[i] = '/';
        }
    }
    delete[] path_;
    return 0;
}


FILE *openfile(const char *path, const char *modes, size_t buffer) {
    FILE *file = fopen(path, modes);
    if (!file) {
        fprintf(stderr, "fail to open %s\n", path);
    }
    setvbuf(file, nullptr, _IOFBF, buffer);
    return file;
}


size_t getfilesize(const char *path) {
#if defined(_WIN32)
    HANDLE hfile = CreateFile(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    LARGE_INTEGER size;
    GetFileSizeEx(hfile, &size);
    CloseHandle(hfile);
    return size.QuadPart;
#else
    FILE *file = openfile(path, "rb");
    fseeko(file, 0, SEEK_END);
    auto size = ftello(file);
    fclose(file);
    return size;
#endif
}

size_t getfilepos(FILE *file) {
#if defined(_WIN32)
    return _ftelli64(file);
#else
    return ftello(file);
#endif
}


size_t readlines(char *dst, size_t len, size_t tolerance, FILE *src) {
    size_t read = fread(dst, sizeof(char), len, src);
    dst[read] = 0;
    if (read == len) {
        fgets(dst + read, tolerance, src);
    }
    while (dst[read++]);  // reach the end of data
    return --read;
}
