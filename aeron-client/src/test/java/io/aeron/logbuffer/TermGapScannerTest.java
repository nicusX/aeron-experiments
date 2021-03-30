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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.BitUtil.align;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TermGapScannerTest
{
    private static final int LOG_BUFFER_CAPACITY = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int TERM_ID = 1;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

    private final UnsafeBuffer termBuffer = mock(UnsafeBuffer.class);
    private final TermGapScanner.GapHandler gapHandler = mock(TermGapScanner.GapHandler.class);

    @BeforeEach
    public void setUp()
    {
        when(termBuffer.capacity()).thenReturn(LOG_BUFFER_CAPACITY);
    }

    @Test
    public void shouldReportGapAtBeginningOfBuffer()
    {
        final int frameOffset = align(HEADER_LENGTH * 3, FRAME_ALIGNMENT);
        final int highWaterMark = frameOffset + align(HEADER_LENGTH, FRAME_ALIGNMENT);

        when(termBuffer.getIntVolatile(frameOffset)).thenReturn(HEADER_LENGTH);

        assertEquals(0, TermGapScanner.scanForGap(termBuffer, TERM_ID, 0, highWaterMark, gapHandler));

        verify(gapHandler).onGap(TERM_ID, 0, frameOffset);
    }

    @Test
    public void shouldReportSingleGapWhenBufferNotFull()
    {
        final int tail = align(HEADER_LENGTH, FRAME_ALIGNMENT);
        final int highWaterMark = FRAME_ALIGNMENT * 3;

        when(termBuffer.getIntVolatile(tail - align(HEADER_LENGTH, FRAME_ALIGNMENT))).thenReturn(HEADER_LENGTH);
        when(termBuffer.getIntVolatile(tail)).thenReturn(0);
        when(termBuffer.getIntVolatile(highWaterMark - align(HEADER_LENGTH, FRAME_ALIGNMENT)))
            .thenReturn(HEADER_LENGTH);

        assertEquals(tail, TermGapScanner.scanForGap(termBuffer, TERM_ID, tail, highWaterMark, gapHandler));

        verify(gapHandler).onGap(TERM_ID, tail, align(HEADER_LENGTH, FRAME_ALIGNMENT));
    }

    @Test
    public void shouldReportSingleGapWhenBufferIsFull()
    {
        final int tail = LOG_BUFFER_CAPACITY - (align(HEADER_LENGTH, FRAME_ALIGNMENT) * 2);
        final int highWaterMark = LOG_BUFFER_CAPACITY;

        when(termBuffer.getIntVolatile(tail - align(HEADER_LENGTH, FRAME_ALIGNMENT))).thenReturn(HEADER_LENGTH);
        when(termBuffer.getIntVolatile(tail)).thenReturn(0);
        when(termBuffer.getIntVolatile(highWaterMark - align(HEADER_LENGTH, FRAME_ALIGNMENT)))
            .thenReturn(HEADER_LENGTH);

        assertEquals(tail, TermGapScanner.scanForGap(termBuffer, TERM_ID, tail, highWaterMark, gapHandler));

        verify(gapHandler).onGap(TERM_ID, tail, align(HEADER_LENGTH, FRAME_ALIGNMENT));
    }

    @Test
    public void shouldReportNoGapWhenHwmIsInPadding()
    {
        final int paddingLength = align(HEADER_LENGTH, FRAME_ALIGNMENT) * 2;
        final int tail = LOG_BUFFER_CAPACITY - paddingLength;
        final int highWaterMark = LOG_BUFFER_CAPACITY - paddingLength + HEADER_LENGTH;

        when(termBuffer.getIntVolatile(tail)).thenReturn(paddingLength);
        when(termBuffer.getIntVolatile(tail + HEADER_LENGTH)).thenReturn(0);

        assertEquals(
            LOG_BUFFER_CAPACITY, TermGapScanner.scanForGap(termBuffer, TERM_ID, tail, highWaterMark, gapHandler));

        verifyNoInteractions(gapHandler);
    }
}
