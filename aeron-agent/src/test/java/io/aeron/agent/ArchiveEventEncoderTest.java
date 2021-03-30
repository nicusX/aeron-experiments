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
import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ArchiveEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ArchiveEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);

    @Test
    void testEncodeSessionStateChange()
    {
        final int offset = 24;
        final TimeUnit from = DAYS;
        final TimeUnit to = MILLISECONDS;
        final long sessionId = Long.MAX_VALUE;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int length = payload.length() + SIZE_OF_LONG + SIZE_OF_INT;
        final int captureLength = captureLength(length);

        final int encodedLength = encodeSessionStateChange(
            buffer, offset, captureLength, length, from, to, sessionId);

        assertEquals(encodedLength(sessionStateChangeLength(from, to)), encodedLength);
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getLong(offset + LOG_HEADER_LENGTH));
        assertEquals(payload, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG));
    }

    @Test
    void testSessionStateChangeLength()
    {
        final ChronoUnit from = ChronoUnit.ERAS;
        final ChronoUnit to = ChronoUnit.MILLENNIA;
        final String payload = from.name() + STATE_SEPARATOR + to.name();

        assertEquals(payload.length() + SIZE_OF_LONG + SIZE_OF_INT, sessionStateChangeLength(from, to));
    }

    @Test
    void testEncodeReplaySessionError()
    {
        final int offset = 24;
        final long sessionId = Long.MAX_VALUE;
        final long recordingId = 56;
        final String errorMessage = "funny";
        final int length = errorMessage.length() + SIZE_OF_LONG * 2 + SIZE_OF_INT;
        final int captureLength = captureLength(length);

        encodeReplaySessionError(buffer, offset, captureLength, length, sessionId, recordingId, errorMessage);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(sessionId, buffer.getLong(offset + LOG_HEADER_LENGTH));
        assertEquals(recordingId, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG));
        assertEquals(errorMessage, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG * 2));
    }

    @Test
    void testEncodeCatalogResize()
    {
        final int offset = 24;
        final int length = SIZE_OF_LONG * 2 + SIZE_OF_INT * 2;
        final int captureLength = captureLength(length);
        final long catalogLength = 128;
        final long newCatalogLength = 1024;

        encodeCatalogResize(buffer, offset, captureLength, length, catalogLength, newCatalogLength);

        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(catalogLength, buffer.getLong(offset + LOG_HEADER_LENGTH));
        assertEquals(newCatalogLength, buffer.getLong(offset + LOG_HEADER_LENGTH + SIZE_OF_LONG));
    }
}
