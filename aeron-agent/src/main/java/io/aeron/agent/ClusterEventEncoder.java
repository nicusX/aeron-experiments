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
package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.agent.CommonEventEncoder.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

final class ClusterEventEncoder
{
    private ClusterEventEncoder()
    {
    }

    static int encodeNewLeadershipTerm(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long logLeadershipTermId,
        final long logTruncatePosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + relativeOffset, logLeadershipTermId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, logTruncatePosition, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, leadershipTermId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, termBaseLogPosition, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, logPosition, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, leaderRecordingId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, timestamp, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + relativeOffset, leaderMemberId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, logSessionId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, isStartup ? 1 : 0, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        return relativeOffset;
    }

    static int newLeaderShipTermLength()
    {
        return (SIZE_OF_LONG * 7) + (SIZE_OF_INT * 3);
    }

    static <E extends Enum<E>> int encodeStateChange(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final E from,
        final E to,
        final int memberId)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putInt(offset + relativeOffset, memberId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        encodingBuffer.putInt(offset + relativeOffset, captureLength - (SIZE_OF_INT * 2), LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        final String fromName = null == from ? "null" : from.name();
        final String toName = null == to ? "null" : to.name();
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, fromName);
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, STATE_SEPARATOR);
        relativeOffset += encodingBuffer.putStringWithoutLengthAscii(offset + relativeOffset, toName);

        return relativeOffset;
    }

    static <E extends Enum<E>> int stateChangeLength(final E from, final E to)
    {
        return stateTransitionStringLength(from, to) + SIZE_OF_INT;
    }

    static int encodeCanvassPosition(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long logLeadershipTermId,
        final long logPosition,
        final int followerMemberId)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + relativeOffset, logLeadershipTermId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, logPosition, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + relativeOffset, followerMemberId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        return relativeOffset;
    }

    static int canvassPositionLength()
    {
        return (2 * SIZE_OF_LONG) + SIZE_OF_INT;
    }

    static int encodeRequestVote(
        final UnsafeBuffer encodingBuffer,
        final int offset,
        final int captureLength,
        final int length,
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId)
    {
        int relativeOffset = encodeLogHeader(encodingBuffer, offset, captureLength, length);

        encodingBuffer.putLong(offset + relativeOffset, logLeadershipTermId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, logPosition, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putLong(offset + relativeOffset, candidateTermId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_LONG;

        encodingBuffer.putInt(offset + relativeOffset, candidateId, LITTLE_ENDIAN);
        relativeOffset += SIZE_OF_INT;

        return relativeOffset;
    }

    static int requestVoteLength()
    {
        return (3 * SIZE_OF_LONG) + SIZE_OF_INT;
    }
}
