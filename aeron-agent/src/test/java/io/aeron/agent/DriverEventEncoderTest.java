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

import io.aeron.cluster.codecs.ClusterTimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoField;

import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.DriverEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DriverEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH * 10]);

    @Test
    void encodePublicationRemovalShouldWriteUriLast()
    {
        final int offset = 10;
        final String uri = "aeron:udp?endpoint=224.10.9.8";
        final int sessionId = 42;
        final int streamId = 5;
        final int captureLength = 3 * SIZE_OF_INT + uri.length();

        encodePublicationRemoval(buffer, offset, captureLength, captureLength, uri, sessionId, streamId);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(captureLength, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getInt(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodePublicationRemovalShouldTruncateUriIfItExceedsMaxMessageLength()
    {
        final int offset = 121;
        final char[] data = new char[MAX_EVENT_LENGTH];
        fill(data, 'z');
        final String uri = new String(data);
        final int length = data.length + 3 * SIZE_OF_INT;
        final int captureLength = captureLength(length);

        encodePublicationRemoval(buffer, offset, captureLength, length, uri, 1, -1);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(1, buffer.getInt(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(-1, buffer.getInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri.substring(0, captureLength - 3 * SIZE_OF_INT - 3) + "...",
            buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }

    @Test
    void encodeSubscriptionRemovalShouldWriteUriLast()
    {
        final int offset = 0;
        final String uri = "aeron:udp?endpoint=224.10.9.8";
        final int streamId = 13;
        final long id = Long.MAX_VALUE;
        final int captureLength = 2 * SIZE_OF_INT + SIZE_OF_LONG + uri.length();

        encodeSubscriptionRemoval(buffer, offset, captureLength, captureLength, uri, streamId, id);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(captureLength, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(id, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri,
            buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void encodeSubscriptionRemovalShouldTruncateUriIfItExceedsMaxMessageLength()
    {
        final char[] data = new char[MAX_EVENT_LENGTH * 3 + 5];
        fill(data, 'a');
        final int offset = 0;
        final int length = SIZE_OF_INT * 2 + SIZE_OF_LONG + data.length;
        final int captureLength = captureLength(length);
        final String uri = new String(data);
        final int streamId = 1;
        final long id = -1;

        encodeSubscriptionRemoval(buffer, offset, captureLength, length, uri, streamId, id);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(id, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(uri.substring(0, captureLength - SIZE_OF_INT * 2 - SIZE_OF_LONG - 3) + "...",
            buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void encodeImageRemovalShouldWriteUriLast()
    {
        final int offset = 0;
        final String uri = "aeron:udp?endpoint=224.10.9.8";
        final int sessionId = 13;
        final int streamId = 42;
        final long id = Long.MAX_VALUE;
        final int captureLength = 3 * SIZE_OF_INT + SIZE_OF_LONG + uri.length();

        encodeImageRemoval(buffer, offset, captureLength, captureLength, uri, sessionId, streamId, id);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(captureLength, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getInt(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(id, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(uri,
            buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void encodeImageRemovalShouldTruncateUriIfItExceedsMaxMessageLength()
    {
        final char[] data = new char[MAX_EVENT_LENGTH + 8];
        fill(data, 'a');
        final int offset = 0;
        final int length = data.length + SIZE_OF_LONG + SIZE_OF_INT * 3;
        final int captureLength = captureLength(length);
        final String uri = new String(data);
        final int sessionId = -1;
        final int streamId = 1;
        final long id = 0;

        encodeImageRemoval(buffer, offset, captureLength, length, uri, sessionId, streamId, id);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getInt(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + LOG_HEADER_LENGTH + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(id, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(uri.substring(0, captureLength - SIZE_OF_LONG - SIZE_OF_INT * 3 - 3) + "...",
            buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT * 2 + SIZE_OF_LONG, LITTLE_ENDIAN));
    }

    @Test
    void untetheredSubscriptionStateChangeLengthComputesLengthBasedOnProvidedState()
    {
        final ClusterTimeUnit from = ClusterTimeUnit.MILLIS;
        final ClusterTimeUnit to = ClusterTimeUnit.NANOS;

        assertEquals(stateTransitionStringLength(from, to) + SIZE_OF_LONG + 2 * SIZE_OF_INT,
            untetheredSubscriptionStateChangeLength(from, to));
    }

    @Test
    void encodeUntetheredSubscriptionStateChangeShouldEncodeStateChangeLast()
    {
        final int offset = 0;
        final ChronoField from = ChronoField.ALIGNED_DAY_OF_WEEK_IN_MONTH;
        final ChronoField to = ChronoField.AMPM_OF_DAY;
        final int length = untetheredSubscriptionStateChangeLength(from, to);
        final int captureLength = captureLength(length);
        final long subscriptionId = 1_010_010_000_010L;
        final int sessionId = 42;
        final int streamId = Integer.MIN_VALUE;

        encodeUntetheredSubscriptionStateChange(
            buffer, offset, captureLength, length, from, to, subscriptionId, streamId, sessionId);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(subscriptionId, buffer.getLong(offset + LOG_HEADER_LENGTH, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getInt(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT, LITTLE_ENDIAN));
        assertEquals(from.name() + STATE_SEPARATOR + to.name(),
            buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG + SIZE_OF_INT * 2, LITTLE_ENDIAN));
    }
}
