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

#ifndef AERON_CONCURRENT_LOGBUFFER_TERM_SCANNER_H
#define AERON_CONCURRENT_LOGBUFFER_TERM_SCANNER_H

#include "util/BitUtil.h"
#include "concurrent/logbuffer/FrameDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer
{

namespace TermScanner
{

inline std::int64_t scanOutcome(std::int32_t padding, std::int32_t available)
{
    return (static_cast<std::int64_t>(padding) << 32) | available;
}

inline std::int32_t available(std::int64_t scanOutcome)
{
    return static_cast<std::int32_t>(scanOutcome);
}

inline std::int32_t padding(std::int64_t scanOutcome)
{
    return static_cast<std::int32_t>(scanOutcome >> 32);
}

inline std::int64_t scanForAvailability(AtomicBuffer &termBuffer, std::int32_t offset, std::int32_t maxLength)
{
    maxLength = std::min(maxLength, termBuffer.capacity() - offset);
    std::int32_t available = 0;
    std::int32_t padding = 0;

    do
    {
        const util::index_t frameOffset = offset + available;
        const util::index_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, frameOffset);
        if (frameLength <= 0)
        {
            break;
        }

        util::index_t alignedFrameLength = util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
        if (FrameDescriptor::isPaddingFrame(termBuffer, frameOffset))
        {
            padding = alignedFrameLength - DataFrameHeader::LENGTH;
            alignedFrameLength = DataFrameHeader::LENGTH;
        }

        available += alignedFrameLength;

        if (available > maxLength)
        {
            available -= alignedFrameLength;
            padding = 0;
            break;
        }
    }
    while ((available + padding) < maxLength);

    return scanOutcome(padding, available);
}

}

}}}

#endif
