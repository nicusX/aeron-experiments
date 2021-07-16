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

#ifndef AERON_AERON_DRIVER_URI_H
#define AERON_AERON_DRIVER_URI_H

#include "uri/aeron_uri.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

typedef struct aeron_driver_uri_publication_params_stct
{
    bool has_position;
    bool is_sparse;
    bool signal_eos;
    bool spies_simulate_connection;
    size_t mtu_length;
    size_t term_length;
    size_t term_offset;
    int32_t initial_term_id;
    int32_t term_id;
    uint64_t linger_timeout_ns;
    bool has_session_id;
    int32_t session_id;
    int64_t entity_tag;
}
aeron_driver_uri_publication_params_t;

typedef struct aeron_driver_uri_subscription_params_stct
{
    bool is_reliable;
    bool is_sparse;
    bool is_tether;
    bool is_rejoin;
    aeron_inferable_boolean_t group;
    bool has_session_id;
    int32_t session_id;
    size_t initial_window_length;
    int32_t packet_timestamp_offset;
}
aeron_driver_uri_subscription_params_t;

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

int aeron_diver_uri_publication_params(
    aeron_uri_t *uri,
    aeron_driver_uri_publication_params_t *params,
    aeron_driver_conductor_t *conductor,
    bool is_exclusive);

int aeron_driver_uri_subscription_params(
    aeron_uri_t *uri,
    aeron_driver_uri_subscription_params_t *params,
    aeron_driver_conductor_t *conductor);

int aeron_publication_params_validate_mtu_for_sndbuf(
    aeron_driver_uri_publication_params_t *params,
    size_t endpoint_socket_sndbuf,
    size_t os_default_socket_sndbuf);

int aeron_subscription_params_validate_initial_window_for_rcvbuf(
    aeron_driver_uri_subscription_params_t *params,
    size_t endpoint_socket_rcvbuf,
    size_t os_default_socket_rcvbuf);

int aeron_driver_uri_get_packet_timestamp_offset(aeron_uri_t *uri, int32_t *offset);
const char *aeron_driver_uri_get_offset_info(int32_t offset);


#endif //AERON_AERON_DRIVER_URI_H
