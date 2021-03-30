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

#ifndef AERON_LOG_BUFFER_UNBLOCK_H
#define AERON_LOG_BUFFER_UNBLOCK_H

#include "util/aeron_fileutil.h"
#include "concurrent/aeron_term_unblocker.h"

bool aeron_logbuffer_unblocker_unblock(
    aeron_mapped_buffer_t *term_buffers, aeron_logbuffer_metadata_t *log_meta_data, int64_t blocked_position);

#endif //AERON_LOG_BUFFER_UNBLOCK_H
