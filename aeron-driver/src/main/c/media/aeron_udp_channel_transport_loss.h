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

#ifndef AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H
#define AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H

#include "aeron_udp_channel_transport_bindings.h"

typedef struct aeron_udp_channel_interceptor_loss_params_stct
{
    double rate;
    unsigned long recv_msg_type_mask;
    unsigned long long seed;
}
aeron_udp_channel_interceptor_loss_params_t;

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_loss_load(
    const aeron_udp_channel_interceptor_bindings_t *delegate_bindings);

void aeron_udp_channel_interceptor_loss_incoming(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_udp_channel_interceptor_loss_configure(const aeron_udp_channel_interceptor_loss_params_t *loss_params);

int aeron_udp_channel_interceptor_loss_parse_params(char *uri, aeron_udp_channel_interceptor_loss_params_t *params);

int aeron_udp_channel_interceptor_loss_parse_callback(void *clientd, const char *key, const char *value);

#endif //AERON_AERON_UDP_CHANNEL_TRANSPORT_LOSS_H
