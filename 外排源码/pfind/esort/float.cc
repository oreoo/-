#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "bcd_table.h"
#include "float.h"


uint64_t float_encode(const char *src, char **end_ptr) {
    int i = -1 + (src[0] == '-' || src[0] == '+');
    int n = 11, e = 0;
    char digits[12] = "00000000000";
    while (src[++i] == '0');  // all leading 0
    while (n && '0' <= src[i] && src[i] <= '9') {  // at most n effective digits
        digits[--n] = src[i++];
    }
    while ('0' <= src[i] && src[i] <= '9') {  // remaining digits
        ++i;
        --n;
    }
    e = 11 - n - 1;
    if (src[i] == '.') {
        if (n == 11) {  // no digit before the point, update exp `e`
            e = i;  // to measure the shifting of i (or, the num of leading zeros after `.`)
            while (src[++i] == '0');  // all leading 0
            e -= i;
        } else {
            ++i;
        }
        while (n > 0 && '0' <= src[i] && src[i] <= '9') { // remaining effective digits
            digits[--n] = src[i++];
        }
        while ('0' <= src[i] && src[i] <= '9') { // remaining ineffective digits
            ++i;
        }
    }
    if (src[i] == 'e' || src[i] == 'E') {
        int e_ = 0;
        int e_sign = (src[++i] == '-');
        i -= (!e_sign && src[i] != '+');
        while (src[++i] == '0');
        while ('0' <= src[i] && src[i] <= '9') {
            e_ *= 10;
            e_ += src[i++] & 0x0f;
        }
        e += e_sign ? -e_ : e_;
    }
    if (n == 11) {  // exp = 0 if all digits are 0
        e = 0;
    }
    *end_ptr = const_cast<char *>(src + i);

    if (digits[0] >= '5') {
        ++(digits[1]);
    }
    int r = 1;
    while (digits[r] == ('9' + 1)) {
        digits[r] = '0';
        ++(digits[++r]);
    }
    if (digits[11]) {
        digits[10] = '1';
        ++e;
    }

    // -*E+* < -*E-* < +0E+0 < +*E-* < +*E+*
    const static uint64_t codes[] = {
            0b0010u << 12u,  // 000 -*E-*
            0b0001u << 12u,  // 001 -*E+*
            0b1011u << 12u,  // 010 +*E-*
            0b1100u << 12u,  // 011 +*E+*
            0b1000u << 12u,  // 100 0
            0b1000u << 12u,  // 101 0
            0b1000u << 12u,  // 110 0
            0b1000u << 12u,  // 111 0
    };
    uint64_t d = codes[((n == 11) << 2u) | ((src[0] != '-') << 1u) | (e >= 0)] | int_bcd3_table[abs(e)];
    if ((d & 0x1000u)) {  // reverse
        d ^= 0xfffu;
    }

    // digits
    d <<= 40u;
    ((uint8_t *) (&d))[4] = char_bcd2_table[*((uint16_t *) (digits + 9))];
    ((uint8_t *) (&d))[3] = char_bcd2_table[*((uint16_t *) (digits + 7))];
    ((uint8_t *) (&d))[2] = char_bcd2_table[*((uint16_t *) (digits + 5))];
    ((uint8_t *) (&d))[1] = char_bcd2_table[*((uint16_t *) (digits + 3))];
    ((uint8_t *) (&d))[0] = char_bcd2_table[*((uint16_t *) (digits + 1))];
    if (!(d & 0x80000000000000u)) {  // neg digits, reverse
        d ^= 0xffffffffffu;
    }
    return d;
}


size_t floats_encode(uint64_t *dst, int n, const char *src, char **end_ptr, size_t *invalid) {
    size_t i = 0;
    while (i < n) {
        dst[i] = float_encode(src, end_ptr);
        if (**end_ptr == '\n') {
            ++i;
        } else if (**end_ptr == 0) {
            break;
        } else {  // invalid item
            ++(*invalid);
            while (*(++(*end_ptr)) != '\n');  // move to next one
            **end_ptr = 0;
            printf("invalid item: %s\n", src);
        }
        src = *end_ptr + 1;
    }
    return i;
}


char *float_decode(char *dst, uint64_t src) {
    if (!(src & 0x80000000000000u)) {
        *(dst++) = '-';
        src ^= 0xffffffffffu;
    }
    dst[0] = (src >> 36u & 0xfu) | 0x30u;
    dst[1] = '.';
    dst[2] = (src >> 32u & 0xfu) | 0x30u;
    *((uint16_t *) (dst + 3)) = bcd_char2_table[((uint8_t *) (&src))[3]];
    *((uint16_t *) (dst + 5)) = bcd_char2_table[((uint8_t *) (&src))[2]];
    *((uint16_t *) (dst + 7)) = bcd_char2_table[((uint8_t *) (&src))[1]];
    *((uint16_t *) (dst + 9)) = bcd_char2_table[((uint8_t *) (&src))[0]];
    dst[11] = 'E';
    if (src & (uint64_t(0x2000u) << 40u)) {
        dst[12] = '-';
    } else {
        dst[12] = '+';
    }
    if ((src & (uint64_t(0x1000u) << 40u))) {  // reverse
        src ^= (uint64_t(0xfffu) << 40u);
    }
    dst[13] = (src & 0xf000000000000u) >> 48u | 0x30u;
    *((uint16_t *) (dst + 14)) = bcd_char2_table[((uint8_t *) (&src))[5]];
    dst[16] = '\n';
    return dst + 17;
}


char *floats_decode(char *dst, int n, const uint64_t *src) {
    for (size_t i = 0; i < n; ++i) {
        dst = float_decode(dst, src[i]);
    }
    return dst;
}
