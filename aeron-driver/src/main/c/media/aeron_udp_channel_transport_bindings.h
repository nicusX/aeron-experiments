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

#ifndef AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H

#include "uri/aeron_driver_uri.h"
#include "aeron_socket.h"
#include "aeron_driver_common.h"

#define AERON_UDP_CHANNEL_TRANSPORT_MAX_INTERCEPTORS (2)

typedef enum aeron_udp_channel_transport_affinity_en
{
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_SENDER,
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER,
    AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_CONDUCTOR
}
aeron_udp_channel_transport_affinity_t;

struct mmsghdr;
typedef struct aeron_udp_channel_transport_stct aeron_udp_channel_transport_t;
typedef struct aeron_network_publication_stct aeron_network_publication_t;
typedef struct aeron_publication_image_stct aeron_publication_image_t;
typedef struct aeron_udp_transport_poller_stct aeron_udp_transport_poller_t;
typedef struct aeron_udp_channel_data_paths_stct aeron_udp_channel_data_paths_t;
typedef struct aeron_data_packet_dispatcher_stct aeron_data_packet_dispatcher_t;

typedef int (*aeron_udp_channel_transport_init_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

typedef int (*aeron_udp_channel_transport_close_func_t)(aeron_udp_channel_transport_t *transport);

typedef void (*aeron_udp_transport_recv_func_t)(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

typedef int (*aeron_udp_channel_transport_recvmmsg_func_t)(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd);

typedef int (*aeron_udp_channel_transport_sendmmsg_func_t)(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

typedef int (*aeron_udp_channel_transport_sendmsg_func_t)(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);

typedef int (*aeron_udp_channel_transport_get_so_rcvbuf_func_t)(
    aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf);

typedef int (*aeron_udp_channel_transport_bind_addr_and_port_func_t)(
    aeron_udp_channel_transport_t *transport, char *buffer, size_t length);

typedef int (*aeron_udp_transport_poller_init_func_t)(
    aeron_udp_transport_poller_t *poller,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);
typedef int (*aeron_udp_transport_poller_close_func_t)(aeron_udp_transport_poller_t *poller);

typedef int (*aeron_udp_transport_poller_add_func_t)(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);
typedef int (*aeron_udp_transport_poller_remove_func_t)(
    aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport);

typedef int (*aeron_udp_transport_poller_poll_func_t)(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd);

typedef struct aeron_udp_channel_transport_bindings_stct aeron_udp_channel_transport_bindings_t;

struct aeron_udp_channel_transport_bindings_stct
{
    aeron_udp_channel_transport_init_func_t init_func;
    aeron_udp_channel_transport_close_func_t close_func;
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func;
    aeron_udp_channel_transport_sendmmsg_func_t sendmmsg_func;
    aeron_udp_channel_transport_sendmsg_func_t sendmsg_func;
    aeron_udp_channel_transport_get_so_rcvbuf_func_t get_so_rcvbuf_func;
    aeron_udp_channel_transport_bind_addr_and_port_func_t bind_addr_and_port_func;
    aeron_udp_transport_poller_init_func_t poller_init_func;
    aeron_udp_transport_poller_close_func_t poller_close_func;
    aeron_udp_transport_poller_add_func_t poller_add_func;
    aeron_udp_transport_poller_remove_func_t poller_remove_func;
    aeron_udp_transport_poller_poll_func_t poller_poll_func;
    struct meta_info_fields
    {
        const char *name;
        const char *type;
        const aeron_udp_channel_transport_bindings_t *next_binding;
        const void *source_symbol;
    }
    meta_info;
};

aeron_udp_channel_transport_bindings_t *aeron_udp_channel_transport_bindings_load_media(const char *bindings_name);

typedef struct aeron_udp_channel_interceptor_bindings_stct aeron_udp_channel_interceptor_bindings_t;

typedef aeron_udp_channel_interceptor_bindings_t *(aeron_udp_channel_interceptor_bindings_load_func_t)(
    aeron_udp_channel_interceptor_bindings_t *delegate_bindings);

aeron_udp_channel_interceptor_bindings_t *aeron_udp_channel_interceptor_bindings_load(
    aeron_udp_channel_interceptor_bindings_t *existing_interceptor_bindings,
    const char *interceptors);

typedef struct aeron_udp_channel_outgoing_interceptor_stct aeron_udp_channel_outgoing_interceptor_t;
typedef struct aeron_udp_channel_incoming_interceptor_stct aeron_udp_channel_incoming_interceptor_t;

typedef enum aeron_udp_channel_interceptor_notification_type_en
{
    AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION,
    AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION
}
aeron_udp_channel_interceptor_notification_type_t;

typedef int (*aeron_udp_channel_interceptor_outgoing_mmsg_func_t)(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen);

typedef int (*aeron_udp_channel_interceptor_outgoing_msg_func_t)(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message);

typedef void (*aeron_udp_channel_interceptor_incoming_func_t)(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

typedef int (*aeron_udp_channel_interceptor_init_func_t)(
    void **interceptor_state,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

typedef int (*aeron_udp_channel_interceptor_close_func_t)(
    void *interceptor_state);

typedef int (*aeron_udp_channel_interceptor_transport_notification_func_t)(
    void *interceptor_state,
    aeron_udp_channel_transport_t *transport,
    const aeron_udp_channel_t *udp_channel,
    aeron_data_packet_dispatcher_t *data_packet_dispatcher,
    aeron_udp_channel_interceptor_notification_type_t type);

typedef int (*aeron_udp_channel_interceptor_publication_notification_func_t)(
    void *interceptor_state,
    aeron_udp_channel_transport_t *transport,
    aeron_network_publication_t *publication,
    aeron_udp_channel_interceptor_notification_type_t type);

typedef int (*aeron_udp_channel_interceptor_image_notification_func_t)(
    void *interceptor_state,
    aeron_udp_channel_transport_t *transport,
    aeron_publication_image_t *image,
    aeron_udp_channel_interceptor_notification_type_t type);

struct aeron_udp_channel_interceptor_bindings_stct
{
    aeron_udp_channel_interceptor_init_func_t outgoing_init_func;
    aeron_udp_channel_interceptor_init_func_t incoming_init_func;
    aeron_udp_channel_interceptor_outgoing_mmsg_func_t outgoing_mmsg_func;
    aeron_udp_channel_interceptor_outgoing_msg_func_t outgoing_msg_func;
    aeron_udp_channel_interceptor_incoming_func_t incoming_func;
    aeron_udp_channel_interceptor_close_func_t outgoing_close_func;
    aeron_udp_channel_interceptor_close_func_t incoming_close_func;
    aeron_udp_channel_interceptor_transport_notification_func_t outgoing_transport_notification_func;
    aeron_udp_channel_interceptor_transport_notification_func_t incoming_transport_notification_func;
    aeron_udp_channel_interceptor_publication_notification_func_t outgoing_publication_notification_func;
    aeron_udp_channel_interceptor_publication_notification_func_t incoming_publication_notification_func;
    aeron_udp_channel_interceptor_image_notification_func_t outgoing_image_notification_func;
    aeron_udp_channel_interceptor_image_notification_func_t incoming_image_notification_func;
    struct interceptor_meta_info_fields
    {
        const char *name;
        const char *type;
        const aeron_udp_channel_interceptor_bindings_t *next_interceptor_bindings;
        const void *source_symbol;
    }
    meta_info;
};

struct aeron_udp_channel_outgoing_interceptor_stct
{
    void *interceptor_state;
    aeron_udp_channel_interceptor_outgoing_mmsg_func_t outgoing_mmsg_func;
    aeron_udp_channel_interceptor_outgoing_msg_func_t outgoing_msg_func;
    aeron_udp_channel_interceptor_close_func_t close_func;
    aeron_udp_channel_interceptor_transport_notification_func_t outgoing_transport_notification_func;
    aeron_udp_channel_interceptor_publication_notification_func_t outgoing_publication_notification_func;
    aeron_udp_channel_interceptor_image_notification_func_t outgoing_image_notification_func;
    aeron_udp_channel_outgoing_interceptor_t *next_interceptor;
};

struct aeron_udp_channel_incoming_interceptor_stct
{
    void *interceptor_state;
    aeron_udp_channel_interceptor_incoming_func_t incoming_func;
    aeron_udp_channel_interceptor_close_func_t close_func;
    aeron_udp_channel_interceptor_transport_notification_func_t incoming_transport_notification_func;
    aeron_udp_channel_interceptor_publication_notification_func_t incoming_publication_notification_func;
    aeron_udp_channel_interceptor_image_notification_func_t incoming_image_notification_func;
    aeron_udp_channel_incoming_interceptor_t *next_interceptor;
};

struct aeron_udp_channel_data_paths_stct
{
    aeron_udp_channel_outgoing_interceptor_t *outgoing_interceptors;
    aeron_udp_channel_incoming_interceptor_t *incoming_interceptors;
    aeron_udp_channel_transport_sendmmsg_func_t sendmmsg_func;
    aeron_udp_channel_transport_sendmsg_func_t sendmsg_func;
    aeron_udp_transport_recv_func_t recv_func;
};

inline int aeron_udp_channel_outgoing_interceptor_sendmmsg(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    aeron_udp_channel_outgoing_interceptor_t *interceptor = data_paths->outgoing_interceptors;

    /* use first interceptor and pass in delegate */
    return interceptor->outgoing_mmsg_func(
        interceptor->interceptor_state, interceptor->next_interceptor, transport, msgvec, vlen);
}

inline int aeron_udp_channel_outgoing_interceptor_sendmsg(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    aeron_udp_channel_outgoing_interceptor_t *interceptor = data_paths->outgoing_interceptors;

    /* use first interceptor and pass in delegate */
    return interceptor->outgoing_msg_func(
        interceptor->interceptor_state, interceptor->next_interceptor, transport, message);
}

inline int aeron_udp_channel_outgoing_interceptor_mmsg_to_transport(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    aeron_udp_channel_transport_sendmmsg_func_t func =
        ((aeron_udp_channel_transport_bindings_t *)interceptor_state)->sendmmsg_func;

    return func(NULL, transport, msgvec, vlen);
}

inline int aeron_udp_channel_outgoing_interceptor_msg_to_transport(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    aeron_udp_channel_transport_sendmsg_func_t func =
        ((aeron_udp_channel_transport_bindings_t *)interceptor_state)->sendmsg_func;

    return func(NULL, transport, message);
}

inline void aeron_udp_channel_incoming_interceptor_recv_func(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_udp_channel_incoming_interceptor_t *interceptor = data_paths->incoming_interceptors;

    interceptor->incoming_func(
        interceptor->interceptor_state,
        interceptor->next_interceptor,
        transport,
        receiver_clientd,
        endpoint_clientd,
        destination_clientd,
        buffer,
        length,
        addr);
}

inline void aeron_udp_channel_incoming_interceptor_to_endpoint(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
    aeron_udp_transport_recv_func_t func = (aeron_udp_transport_recv_func_t)interceptor_state;
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif

    func(NULL, transport, receiver_clientd, endpoint_clientd, destination_clientd, buffer, length, addr);
}

int aeron_udp_channel_data_paths_init(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_interceptor_bindings_t *outgoing_interceptor_bindings,
    aeron_udp_channel_interceptor_bindings_t *incoming_interceptor_bindings,
    aeron_udp_channel_transport_bindings_t *media_bindings,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity);

int aeron_udp_channel_data_paths_delete(aeron_udp_channel_data_paths_t *data_paths);

inline int aeron_udp_channel_interceptors_transport_notifications(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    const aeron_udp_channel_t *udp_channel,
    aeron_data_packet_dispatcher_t *data_packet_dispatcher,
    aeron_udp_channel_interceptor_notification_type_t type)
{
    for (
        aeron_udp_channel_incoming_interceptor_t *interceptor = data_paths->incoming_interceptors;
        NULL != interceptor;
        interceptor = interceptor->next_interceptor)
    {
        if (NULL != interceptor->incoming_transport_notification_func)
        {
            if (interceptor->incoming_transport_notification_func(
                interceptor->interceptor_state, transport, udp_channel, data_packet_dispatcher, type) < 0)
            {
                return -1;
            }
        }
    }

    for (
        aeron_udp_channel_outgoing_interceptor_t *interceptor = data_paths->outgoing_interceptors;
        NULL != interceptor;
        interceptor = interceptor->next_interceptor)
    {
        if (NULL != interceptor->outgoing_transport_notification_func)
        {
            if (interceptor->outgoing_transport_notification_func(
                interceptor->interceptor_state, transport, udp_channel, data_packet_dispatcher, type) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

inline int aeron_udp_channel_interceptors_publication_notifications(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_network_publication_t *publication,
    aeron_udp_channel_interceptor_notification_type_t type)
{
    for (
        aeron_udp_channel_incoming_interceptor_t *interceptor = data_paths->incoming_interceptors;
        NULL != interceptor;
        interceptor = interceptor->next_interceptor)
    {
        if (NULL != interceptor->incoming_publication_notification_func)
        {
            if (interceptor->incoming_publication_notification_func(
                interceptor->interceptor_state, transport, publication, type) < 0)
            {
                return -1;
            }
        }
    }

    for (
        aeron_udp_channel_outgoing_interceptor_t *interceptor = data_paths->outgoing_interceptors;
        NULL != interceptor;
        interceptor = interceptor->next_interceptor)
    {
        if (NULL != interceptor->outgoing_publication_notification_func)
        {
            if (interceptor->outgoing_publication_notification_func(
                interceptor->interceptor_state, transport, publication, type) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

inline int aeron_udp_channel_interceptors_image_notifications(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    aeron_publication_image_t *image,
    aeron_udp_channel_interceptor_notification_type_t type)
{
    for (
        aeron_udp_channel_incoming_interceptor_t *interceptor = data_paths->incoming_interceptors;
        NULL != interceptor;
        interceptor = interceptor->next_interceptor)
    {
        if (NULL != interceptor->incoming_image_notification_func)
        {
            if (interceptor->incoming_image_notification_func(
                interceptor->interceptor_state, transport, image, type) < 0)
            {
                return -1;
            }
        }
    }

    for (
        aeron_udp_channel_outgoing_interceptor_t *interceptor = data_paths->outgoing_interceptors;
        NULL != interceptor;
        interceptor = interceptor->next_interceptor)
    {
        if (NULL != interceptor->outgoing_image_notification_func)
        {
            if (interceptor->outgoing_image_notification_func(
                interceptor->interceptor_state, transport, image, type) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

#endif //AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_H
