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
#ifndef AERON_COMMAND_SUBSCRIPTION_MESSAGE_FLYWEIGHT_H
#define AERON_COMMAND_SUBSCRIPTION_MESSAGE_FLYWEIGHT_H

#include <cstdint>
#include <string>
#include <cstddef>
#include "CorrelatedMessageFlyweight.h"

namespace aeron { namespace command
{

/**
* Control message for adding a subscription.
*
* <p>
*   0                   1                   2                   3
*   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |                         Client ID                             |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                       Correlation ID                          |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                 Registration Correlation ID                   |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                         Stream Id                             |
*  +---------------------------------------------------------------+
*  |                      Channel Length                           |
*  +---------------------------------------------------------------+
*  |                          Channel                             ...
* ...                                                              |
*  +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct SubscriptionMessageDefn
{
    CorrelatedMessageDefn correlatedMessage;
    std::int64_t registrationCorrelationId;
    std::int32_t streamId;
    std::int32_t channelLength;
    std::int8_t channelData[1];
};
#pragma pack(pop)

class SubscriptionMessageFlyweight : public CorrelatedMessageFlyweight
{
public:
    typedef SubscriptionMessageFlyweight this_t;

    inline SubscriptionMessageFlyweight(concurrent::AtomicBuffer &buffer, util::index_t offset) :
        CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<SubscriptionMessageDefn>(0))
    {
    }

    inline std::int64_t registrationCorrelationId() const
    {
        return m_struct.registrationCorrelationId;
    }

    inline this_t &registrationCorrelationId(std::int64_t value)
    {
        m_struct.registrationCorrelationId = value;
        return *this;
    }

    inline std::int32_t streamId() const
    {
        return m_struct.streamId;
    }

    inline this_t &streamId(std::int32_t value)
    {
        m_struct.streamId = value;
        return *this;
    }

    inline std::string channel() const
    {
        return stringGet(static_cast<util::index_t>(offsetof(SubscriptionMessageDefn, channelLength)));
    }

    inline this_t &channel(const std::string &value)
    {
        stringPut(static_cast<util::index_t>(offsetof(SubscriptionMessageDefn, channelLength)), value);
        return *this;
    }

    inline util::index_t length() const
    {
        return static_cast<util::index_t>(offsetof(SubscriptionMessageDefn, channelData) + m_struct.channelLength);
    }

private:
    SubscriptionMessageDefn &m_struct;
};

}}
#endif
