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

#ifndef AERON_NAME_RESOLVER_DRIVER_H
#define AERON_NAME_RESOLVER_DRIVER_H

#define AERON_NAME_RESOLVER_DRIVER_SELF_RESOLUTION_INTERVAL_MS (1 * INT64_C(1000))
#define AERON_NAME_RESOLVER_DRIVER_NEIGHBOUR_RESOLUTION_INTERVAL_MS (2 * INT64_C(2000))
#define AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS (10 * INT64_C(1000))
#define AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID (15)
#define AERON_COUNTER_NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID (16)

#include "protocol/aeron_udp_protocol.h"

int aeron_driver_name_resolver_set_resolution_header(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    aeron_name_resolver_cache_addr_t *cache_addr,
    const char *name,
    size_t name_length);

int aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
    aeron_resolution_header_t *resolution_header,
    size_t capacity,
    uint8_t flags,
    struct sockaddr_storage *addr,
    const char *name,
    size_t name_length);

#endif //AERON_NAME_RESOLVER_DRIVER_H
