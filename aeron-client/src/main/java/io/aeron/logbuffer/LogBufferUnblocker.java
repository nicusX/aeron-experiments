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
package io.aeron.logbuffer;

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.LogBufferDescriptor.*;

/**
 * Provides the functionality to unblock a log at a given position.
 */
public class LogBufferUnblocker
{
    /**
     * Attempt to unblock a log buffer at given position
     *
     * @param termBuffers       for current blockedOffset
     * @param logMetaDataBuffer for log buffer
     * @param blockedPosition   to attempt to unblock
     * @param termLength        of the buffer for each term in the log
     * @return whether unblocked or not
     */
    public static boolean unblock(
        final UnsafeBuffer[] termBuffers,
        final UnsafeBuffer logMetaDataBuffer,
        final long blockedPosition,
        final int termLength)
    {
        final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        final int blockedTermCount = (int)(blockedPosition >> positionBitsToShift);
        final int blockedOffset = (int)blockedPosition & (termLength - 1);
        final int activeTermCount = activeTermCount(logMetaDataBuffer);

        if (activeTermCount == (blockedTermCount - 1) && blockedOffset == 0)
        {
            final int currentTermId = termId(rawTailVolatile(logMetaDataBuffer, indexByTermCount(activeTermCount)));
            return rotateLog(logMetaDataBuffer, activeTermCount, currentTermId);
        }

        final int blockedIndex = indexByTermCount(blockedTermCount);
        final long rawTail = rawTailVolatile(logMetaDataBuffer, blockedIndex);
        final int termId = termId(rawTail);
        final int tailOffset = termOffset(rawTail, termLength);
        final UnsafeBuffer termBuffer = termBuffers[blockedIndex];

        switch (TermUnblocker.unblock(logMetaDataBuffer, termBuffer, blockedOffset, tailOffset, termId))
        {
            case UNBLOCKED_TO_END:
                rotateLog(logMetaDataBuffer, blockedTermCount, termId);
                // fall through
            case UNBLOCKED:
                return true;
        }

        return false;
    }
}
