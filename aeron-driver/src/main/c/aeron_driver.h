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

#ifndef AERON_DRIVER_H
#define AERON_DRIVER_H

#include "aeron_driver_context.h"
#include "aeron_driver_conductor.h"
#include "aeron_driver_sender.h"
#include "aeron_driver_receiver.h"

#define AERON_AGENT_RUNNER_CONDUCTOR 0
#define AERON_AGENT_RUNNER_SENDER 1
#define AERON_AGENT_RUNNER_RECEIVER 2
#define AERON_AGENT_RUNNER_SHARED_NETWORK 1
#define AERON_AGENT_RUNNER_SHARED 0
#define AERON_AGENT_RUNNER_MAX 3

typedef struct aeron_driver_stct
{
    aeron_driver_context_t *context;
    aeron_driver_conductor_t conductor;
    aeron_driver_sender_t sender;
    aeron_driver_receiver_t receiver;
    aeron_agent_runner_t runners[AERON_AGENT_RUNNER_MAX];
}
aeron_driver_t;

bool aeron_is_driver_active_with_cnc(
    aeron_mapped_file_t *cnc_map, int64_t timeout_ms, int64_t now_ms, aeron_log_func_t log_func);

int32_t aeron_semantic_version_compose(uint8_t major, uint8_t minor, uint8_t patch);

uint8_t aeron_semantic_version_major(int32_t version);

uint8_t aeron_semantic_version_minor(int32_t version);

uint8_t aeron_semantic_version_patch(int32_t version);

#endif //AERON_DRIVER_H
