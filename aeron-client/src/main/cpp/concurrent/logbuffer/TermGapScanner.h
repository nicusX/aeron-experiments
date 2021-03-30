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

#ifndef AERON_CONCURRENT_TERM_GAP_SCANNER_H
#define AERON_CONCURRENT_TERM_GAP_SCANNER_H

#include "util/BitUtil.h"
#include "concurrent/logbuffer/FrameDescriptor.h"

namespace aeron { namespace concurrent { namespace logbuffer {

namespace TermGapScanner {

inline std::int32_t scanForGap(
    AtomicBuffer &termBuffer,
    std::int32_t termId,
    util::index_t rebuildOffset,
    std::int32_t hwmOffset,
    std::function<void(std::int32_t, AtomicBuffer &, std::int32_t, std::int32_t)> handler)
{
    do
    {
        const std::int32_t frameLength = FrameDescriptor::frameLengthVolatile(termBuffer, rebuildOffset);
        if (frameLength <= 0)
        {
            break;
        }

        rebuildOffset += util::BitUtil::align(frameLength, FrameDescriptor::FRAME_ALIGNMENT);
    }
    while (rebuildOffset < hwmOffset);

    const std::int32_t gapBeginOffset = rebuildOffset;
    if (rebuildOffset < hwmOffset)
    {
        const std::int32_t limit = hwmOffset - FrameDescriptor::ALIGNED_HEADER_LENGTH;

        while (rebuildOffset < limit)
        {
            rebuildOffset += FrameDescriptor::FRAME_ALIGNMENT;

            if (0 != termBuffer.getInt32Volatile(rebuildOffset))
            {
                rebuildOffset -= FrameDescriptor::ALIGNED_HEADER_LENGTH;
                break;
            }
        }

        const std::int32_t gapLength = (rebuildOffset - gapBeginOffset) + FrameDescriptor::ALIGNED_HEADER_LENGTH;
        handler(termId, termBuffer, gapBeginOffset, gapLength);
    }

    return gapBeginOffset;
}

}

}}}

#endif
