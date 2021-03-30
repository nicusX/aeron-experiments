/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_STRUTIL_H
#define AERON_STRUTIL_H

#include <stdint.h>
#include <stddef.h>

void aeron_format_date(char *str, size_t count, int64_t timestamp);

#define AERON_FORMAT_NUMBER_TO_LOCALE_STR_LEN (32)
char *aeron_format_number_to_locale(long long value, char *buffer, size_t buffer_len);

#define AERON_FORMAT_HEX_LENGTH(b) ((2 * (b)) + 1)
void aeron_format_to_hex(char *str, size_t str_length, uint8_t *data, size_t data_len);

/*
 * FNV-1a hash function
 *
 * http://www.isthe.com/chongo/tech/comp/fnv/index.html
 */
inline uint64_t aeron_fnv_64a_buf(uint8_t *buf, size_t len)
{
    uint8_t *bp = buf;
    uint8_t *be = bp + len;
    uint64_t hval = 0xcbf29ce484222325ULL;

    while (bp < be)
    {
        hval ^= (uint64_t)*bp++;
#if defined(__GNUC__)
        hval += (hval << 1) + (hval << 4) + (hval << 5) + (hval << 7) + (hval << 8) + (hval << 40);
#else
        hval *= ((uint64_t)0x100000001b3ULL);
#endif
    }

    return hval;
}

#ifdef _MSC_VER
#define strdup _strdup
#endif

/*
 * Splits a null terminated string using the delimiter specified, which is replaced with \0 characters.
 * Each of the tokens is stored in reverse order in the tokens array.
 *
 * Returns the number of tokens found. Or a value < 0 for an error:
 * ERANGE: number of tokens is greater than max_tokens.
 */
int aeron_tokenise(char *input, char delimiter, int max_tokens, char **tokens);

#if defined(AERON_DLL_EXPORTS)
#define AERON_EXPORT __declspec(dllexport)
#else
#define AERON_EXPORT __declspec(dllimport)
#endif

#if defined(_MSC_VER) && !defined(AERON_NO_GETOPT)
AERON_EXPORT extern char *optarg;
AERON_EXPORT extern int optind;

int getopt(int argc, char *const argv[], const char *opt_string);
#endif

#endif //AERON_STRUTIL_H
