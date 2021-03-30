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

#ifndef AERON_CONCURRENT_BROADCAST_COPY_BROADCAST_RECEIVER_H
#define AERON_CONCURRENT_BROADCAST_COPY_BROADCAST_RECEIVER_H

#include <array>
#include <functional>

#include "concurrent/broadcast/BroadcastReceiver.h"

namespace aeron { namespace concurrent { namespace broadcast {

typedef std::array<std::uint8_t, 4096> scratch_buffer_t;

/** The data handler function signature */
typedef std::function<void(std::int32_t, concurrent::AtomicBuffer &, util::index_t, util::index_t)> handler_t;

class CopyBroadcastReceiver
{
public:
    explicit CopyBroadcastReceiver(BroadcastReceiver &receiver) :
        m_receiver(receiver),
        m_scratchBuffer(m_scratch)
    {
    }

    int receive(const handler_t &handler)
    {
        int messagesReceived = 0;
        const long lastSeenLappedCount = m_receiver.lappedCount();

        if (m_receiver.receiveNext())
        {
            if (lastSeenLappedCount != m_receiver.lappedCount())
            {
                throw util::IllegalArgumentException("unable to keep up with broadcast buffer", SOURCEINFO);
            }

            const std::int32_t length = m_receiver.length();
            if (length > m_scratchBuffer.capacity())
            {
                throw util::IllegalStateException(
                    "buffer required size " + std::to_string(length) +
                    " but only has " + std::to_string(m_scratchBuffer.capacity()), SOURCEINFO);
            }

            const std::int32_t msgTypeId = m_receiver.typeId();
            m_scratchBuffer.putBytes(0, m_receiver.buffer(), m_receiver.offset(), length);

            if (!m_receiver.validate())
            {
                throw util::IllegalStateException("unable to keep up with broadcast buffer", SOURCEINFO);
            }

            handler(msgTypeId, m_scratchBuffer, 0, length);

            messagesReceived = 1;
        }

        return messagesReceived;
    }

private:
    AERON_DECL_ALIGNED(scratch_buffer_t m_scratch, 16) = {};
    BroadcastReceiver &m_receiver;
    AtomicBuffer m_scratchBuffer;
};

}}}

#endif
