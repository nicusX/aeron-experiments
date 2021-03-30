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

#ifndef AERON_PUBLICATION_IMAGE_H
#define AERON_PUBLICATION_IMAGE_H

#include "aeron_driver_common.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_congestion_control.h"
#include "aeron_loss_detector.h"
#include "reports/aeron_loss_reporter.h"

typedef enum aeron_publication_image_state_enum
{
    AERON_PUBLICATION_IMAGE_STATE_ACTIVE,
    AERON_PUBLICATION_IMAGE_STATE_DRAINING,
    AERON_PUBLICATION_IMAGE_STATE_LINGER,
    AERON_PUBLICATION_IMAGE_STATE_DONE
}
aeron_publication_image_state_t;

typedef struct aeron_publication_image_connection_stct
{
    struct sockaddr_storage resolved_control_address_for_implicit_unicast_channels;
    aeron_receive_destination_t *destination;  // Not owned.
    struct sockaddr_storage *control_addr;     // Not owned.
    bool is_eos;
    int64_t time_of_last_activity_ns;
    int64_t time_of_last_frame_ns;
}
aeron_publication_image_connection_t;

typedef struct aeron_publication_image_stct
{
    uint8_t padding_before[AERON_CACHE_LINE_LENGTH];

    struct aeron_publication_image_conductor_fields_stct
    {
        bool is_reliable;
        aeron_publication_image_state_t state;
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribable_t subscribable;
        int64_t time_of_last_state_change_ns;
        int64_t liveness_timeout_ns;
        int64_t clean_position;
    }
    conductor_fields;

    uint8_t padding_after[AERON_CACHE_LINE_LENGTH];

    struct image_connection_entries
    {
        size_t length;
        size_t capacity;
        aeron_publication_image_connection_t *array;
    }
    connections;

    struct sockaddr_storage source_address;
    aeron_loss_detector_t loss_detector;

    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_position_t rcv_hwm_position;
    aeron_position_t rcv_pos_position;
    aeron_logbuffer_metadata_t *log_meta_data;

    aeron_receive_channel_endpoint_t *endpoint;
    aeron_congestion_control_strategy_t *congestion_control;
    aeron_clock_func_t nano_clock;
    aeron_clock_func_t epoch_clock;
    aeron_clock_cache_t *cached_clock;

    aeron_loss_reporter_t *loss_reporter;
    aeron_loss_reporter_entry_offset_t loss_reporter_offset;

    char *log_file_name;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t active_term_id;
    int32_t term_length;
    int32_t mtu_length;
    int32_t term_length_mask;
    size_t log_file_name_length;
    size_t position_bits_to_shift;
    aeron_raw_log_close_func_t raw_log_close_func;
    aeron_raw_log_free_func_t raw_log_free_func;
    aeron_untethered_subscription_state_change_func_t untethered_subscription_state_change_func;

    int64_t last_loss_change_number;
    volatile int64_t begin_loss_change;
    volatile int64_t end_loss_change;
    int32_t loss_term_id;
    int32_t loss_term_offset;
    size_t loss_length;

    volatile int64_t begin_sm_change;
    volatile int64_t end_sm_change;
    int64_t next_sm_position;
    int32_t next_sm_receiver_window_length;

    int64_t last_sm_change_number;
    int64_t last_sm_position;
    int64_t last_sm_position_window_limit;
    int64_t time_of_last_sm_ns;
    int64_t sm_timeout_ns;

    int64_t time_of_last_packet_ns;

    bool is_end_of_stream;

    int64_t *heartbeats_received_counter;
    int64_t *flow_control_under_runs_counter;
    int64_t *flow_control_over_runs_counter;
    int64_t *status_messages_sent_counter;
    int64_t *nak_messages_sent_counter;
    int64_t *loss_gap_fills_counter;
}
aeron_publication_image_t;

int aeron_publication_image_create(
    aeron_publication_image_t **image,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    aeron_driver_context_t *context,
    int64_t correlation_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t initial_term_offset,
    aeron_position_t *rcv_hwm_position,
    aeron_position_t *rcv_pos_position,
    aeron_congestion_control_strategy_t *congestion_control,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *source_address,
    int32_t term_buffer_length,
    int32_t sender_mtu_length,
    aeron_loss_reporter_t *loss_reporter,
    bool is_reliable,
    bool is_sparse,
    bool treat_as_multicast,
    aeron_system_counters_t *system_counters);

int aeron_publication_image_close(aeron_counters_manager_t *counters_manager, aeron_publication_image_t *image);

void aeron_publication_image_clean_buffer_to(aeron_publication_image_t *image, int64_t position);

void aeron_publication_image_on_gap_detected(void *clientd, int32_t term_id, int32_t term_offset, size_t length);

void aeron_publication_image_track_rebuild(aeron_publication_image_t *image, int64_t now_ns);

int aeron_publication_image_insert_packet(
    aeron_publication_image_t *image,
    aeron_receive_destination_t *destination,
    int32_t term_id,
    int32_t term_offset,
    const uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_publication_image_on_rttm(
    aeron_publication_image_t *image, aeron_rttm_header_t *header, struct sockaddr_storage *addr);

int aeron_publication_image_send_pending_status_message(aeron_publication_image_t *image, int64_t now_ns);

int aeron_publication_image_send_pending_loss(aeron_publication_image_t *image);

int aeron_publication_image_initiate_rttm(aeron_publication_image_t *image, int64_t now_ns);

int aeron_publication_image_add_destination(aeron_publication_image_t *image, aeron_receive_destination_t *destination);

int aeron_publication_image_remove_destination(aeron_publication_image_t *image, aeron_udp_channel_t *channel);

void aeron_publication_image_add_connection_if_unknown(
    aeron_publication_image_t *image, aeron_receive_destination_t *destination, struct sockaddr_storage *src_addr);

void aeron_publication_image_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_publication_image_t *image, int64_t now_ns, int64_t now_ms);

inline bool aeron_publication_image_is_heartbeat(const uint8_t *buffer, size_t length)
{
    return length == AERON_DATA_HEADER_LENGTH && 0 == ((aeron_frame_header_t *)buffer)->frame_length;
}

inline bool aeron_publication_image_is_end_of_stream(const uint8_t *buffer, size_t length)
{
    return (((aeron_frame_header_t *)buffer)->flags & AERON_DATA_HEADER_EOS_FLAG) != 0;
}

inline bool aeron_publication_image_is_flow_control_under_run(aeron_publication_image_t *image, int64_t packet_position)
{
    const bool is_flow_control_under_run = packet_position < image->last_sm_position;

    if (is_flow_control_under_run)
    {
        aeron_counter_ordered_increment(image->flow_control_under_runs_counter, 1);
    }

    return is_flow_control_under_run;
}

inline bool aeron_publication_image_is_flow_control_over_run(
    aeron_publication_image_t *image, int64_t proposed_position)
{
    const bool is_flow_control_over_run = proposed_position > image->last_sm_position_window_limit;

    if (is_flow_control_over_run)
    {
        aeron_counter_ordered_increment(image->flow_control_over_runs_counter, 1);
    }

    return is_flow_control_over_run;
}

inline void aeron_publication_image_schedule_status_message(
    aeron_publication_image_t *image, int64_t sm_position, int32_t window_length)
{
    const int64_t change_number = image->begin_sm_change + 1;

    AERON_PUT_ORDERED(image->begin_sm_change, change_number);
    aeron_release();
    image->next_sm_position = sm_position;
    image->next_sm_receiver_window_length = window_length;
    AERON_PUT_ORDERED(image->end_sm_change, change_number);
}

inline bool aeron_publication_image_is_drained(aeron_publication_image_t *image)
{
    int64_t rebuild_position = aeron_counter_get(image->rcv_pos_position.value_addr);

    for (size_t i = 0, length = image->conductor_fields.subscribable.length; i < length; i++)
    {
        int64_t position = aeron_counter_get_volatile(image->conductor_fields.subscribable.array[i].value_addr);

        if (position < rebuild_position)
        {
            return false;
        }
    }

    return true;
}

inline bool aeron_publication_image_has_no_subscribers(aeron_publication_image_t *image)
{
    return 0 == image->conductor_fields.subscribable.length;
}

inline bool aeron_publication_image_is_accepting_subscriptions(aeron_publication_image_t *image)
{
    return image->conductor_fields.subscribable.length > 0 &&
        (image->conductor_fields.state == AERON_PUBLICATION_IMAGE_STATE_ACTIVE ||
            (image->conductor_fields.state == AERON_PUBLICATION_IMAGE_STATE_DRAINING &&
                !aeron_publication_image_is_drained(image)));
}

inline void aeron_publication_image_disconnect_endpoint(aeron_publication_image_t *image)
{
    image->endpoint = NULL;
}

inline const char *aeron_publication_image_log_file_name(aeron_publication_image_t *image)
{
    return image->log_file_name;
}

inline int64_t aeron_publication_image_registration_id(aeron_publication_image_t *image)
{
    return image->conductor_fields.managed_resource.registration_id;
}

inline size_t aeron_publication_image_subscriber_count(aeron_publication_image_t *image)
{
    return image->conductor_fields.subscribable.length;
}

inline int64_t aeron_publication_image_join_position(aeron_publication_image_t *image)
{
    int64_t position = *image->rcv_pos_position.value_addr;

    for (size_t i = 0, length = image->conductor_fields.subscribable.length; i < length; i++)
    {
        int64_t sub_pos = aeron_counter_get_volatile(image->conductor_fields.subscribable.array[i].value_addr);

        if (sub_pos < position)
        {
            position = sub_pos;
        }
    }

    return position;
}

#endif //AERON_PUBLICATION_IMAGE_H
