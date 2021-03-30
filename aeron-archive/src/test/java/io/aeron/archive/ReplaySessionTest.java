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
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.archive.checksum.Checksums.crc32;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.fill;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BitUtil.align;
import static org.agrona.BufferUtil.address;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ReplaySessionTest
{
    private static final int RECORDING_ID = 0;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_BUFFER_LENGTH;
    private static final int INITIAL_TERM_ID = 8231773;
    private static final int INITIAL_TERM_OFFSET = 1024;
    private static final long REPLAY_ID = 1;
    private static final long START_POSITION = INITIAL_TERM_OFFSET;
    private static final long JOIN_POSITION = START_POSITION;
    private static final long RECORDING_POSITION = INITIAL_TERM_OFFSET;
    private static final long TIME = 0;
    private static final long CONNECT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final int MTU_LENGTH = 4096;
    private static final int FRAME_LENGTH = 1024;
    private static final int SESSION_ID = 1;
    private static final int STREAM_ID = 1001;
    private static final FileChannel ARCHIVE_DIR_CHANNEL = null;

    private final Image mockImage = mock(Image.class);
    private final ExclusivePublication mockReplayPub = mock(ExclusivePublication.class);
    private final ControlSession mockControlSession = mock(ControlSession.class);
    private final ArchiveConductor mockArchiveConductor = mock(ArchiveConductor.class);
    private final Counter recordingPositionCounter = mock(Counter.class);
    private final UnsafeBuffer replayBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH));

    private int messageCounter = 0;
    private int offerBlockOffset = 0;

    private final RecordingSummary recordingSummary = new RecordingSummary();
    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private final ControlResponseProxy proxy = mock(ControlResponseProxy.class);
    private final CachedEpochClock epochClock = new CachedEpochClock();
    private final Catalog mockCatalog = mock(Catalog.class);
    private final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
    private Archive.Context context;
    private long recordingPosition;

    @BeforeEach
    public void before() throws IOException
    {
        context = new Archive.Context()
            .segmentFileLength(SEGMENT_LENGTH)
            .archiveDir(archiveDir)
            .epochClock(epochClock)
            .countedErrorHandler(countedErrorHandler);

        when(recordingPositionCounter.get()).then((invocation) -> recordingPosition);
        when(mockControlSession.archiveConductor()).thenReturn(mockArchiveConductor);
        when(mockArchiveConductor.catalog()).thenReturn(mockCatalog);
        when(mockArchiveConductor.context()).thenReturn(context);
        when(mockReplayPub.termBufferLength()).thenReturn(TERM_BUFFER_LENGTH);
        when(mockReplayPub.positionBitsToShift())
            .thenReturn(LogBufferDescriptor.positionBitsToShift(TERM_BUFFER_LENGTH));
        when(mockReplayPub.initialTermId()).thenReturn(INITIAL_TERM_ID);
        when(mockReplayPub.availableWindow()).thenReturn((long)TERM_BUFFER_LENGTH / 2);
        when(mockImage.termBufferLength()).thenReturn(TERM_BUFFER_LENGTH);
        when(mockImage.joinPosition()).thenReturn(JOIN_POSITION);

        recordingSummary.recordingId = RECORDING_ID;
        recordingSummary.startPosition = START_POSITION;
        recordingSummary.segmentFileLength = context.segmentFileLength();
        recordingSummary.initialTermId = INITIAL_TERM_ID;
        recordingSummary.termBufferLength = TERM_BUFFER_LENGTH;
        recordingSummary.mtuLength = MTU_LENGTH;
        recordingSummary.streamId = STREAM_ID;
        recordingSummary.sessionId = SESSION_ID;

        final RecordingWriter writer = new RecordingWriter(
            RECORDING_ID, START_POSITION, SEGMENT_LENGTH, mockImage, context);

        writer.init();

        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(TERM_BUFFER_LENGTH, 64));

        final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
        final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
        header.buffer(buffer);

        recordFragment(writer, buffer, headerFwt, header, INITIAL_TERM_OFFSET, FRAME_LENGTH, 0, UNFRAGMENTED,
            HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, INITIAL_TERM_OFFSET + FRAME_LENGTH, FRAME_LENGTH, 1,
            BEGIN_FRAG_FLAG, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, INITIAL_TERM_OFFSET + FRAME_LENGTH * 2, FRAME_LENGTH, 2,
            END_FRAG_FLAG, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, INITIAL_TERM_OFFSET + FRAME_LENGTH * 3, FRAME_LENGTH, 3,
            UNFRAGMENTED, HDR_TYPE_PAD, SESSION_ID);

        writer.close();
        recordingSummary.stopPosition = START_POSITION + 4 * FRAME_LENGTH;
    }

    @AfterEach
    public void after()
    {
        IoUtil.delete(archiveDir, false);
    }

    @Test
    public void verifyRecordingFile()
    {
        try (RecordingReader reader = new RecordingReader(
            recordingSummary, archiveDir, NULL_POSITION, AeronArchive.NULL_LENGTH))
        {
            int fragments = reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(frameFlags(buffer, frameOffset), UNFRAGMENTED);
                },
                1);

            assertEquals(1, fragments);

            fragments = reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(frameFlags(buffer, frameOffset), BEGIN_FRAG_FLAG);
                },
                1);

            assertEquals(1, fragments);

            fragments = reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + 2 * FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(frameType(buffer, frameOffset), HDR_TYPE_DATA);
                    assertEquals(frameFlags(buffer, frameOffset), END_FRAG_FLAG);
                },
                1);

            assertEquals(1, fragments);

            fragments = reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - HEADER_LENGTH;
                    assertEquals(offset, INITIAL_TERM_OFFSET + 3 * FRAME_LENGTH + HEADER_LENGTH);
                    assertEquals(length, FRAME_LENGTH - HEADER_LENGTH);
                    assertEquals(frameType(buffer, frameOffset), HDR_TYPE_PAD);
                    assertEquals(frameFlags(buffer, frameOffset), UNFRAGMENTED);
                },
                1);

            assertEquals(1, fragments);
        }
    }

    @Test
    public void shouldReplayPartialDataFromFile()
    {
        final long correlationId = 1L;
        final int sessionId = Integer.MAX_VALUE;
        final int streamId = Integer.MIN_VALUE;

        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            FRAME_LENGTH,
            correlationId,
            mockReplayPub,
            mockControlSession,
            recordingPositionCounter,
            null))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);
            when(mockReplayPub.sessionId()).thenReturn(sessionId);
            when(mockReplayPub.streamId()).thenReturn(streamId);

            replaySession.doWork();

            assertEquals(replaySession.state(), ReplaySession.State.INIT);

            when(mockReplayPub.isConnected()).thenReturn(true);

            final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
            mockPublication(mockReplayPub, termBuffer);
            assertNotEquals(0, replaySession.doWork());
            assertThat(messageCounter, is(1));

            validateFrame(termBuffer, 0, FRAME_LENGTH, 0, UNFRAGMENTED, sessionId, streamId);
            assertTrue(replaySession.isDone());
        }
    }

    @Test
    public void shouldNotReplayPartialUnalignedDataFromFile()
    {
        final long correlationId = 1L;
        final ReplaySession replaySession = replaySession(
            RECORDING_POSITION + 1,
            FRAME_LENGTH,
            correlationId,
            mockReplayPub,
            mockControlSession,
            recordingPositionCounter,
            null);

        replaySession.doWork();
        assertEquals(ReplaySession.State.DONE, replaySession.state());

        final ControlResponseProxy proxy = mock(ControlResponseProxy.class);
        replaySession.sendPendingError(proxy);
        verify(mockControlSession).attemptErrorResponse(eq(correlationId), anyString(), eq(proxy));
    }

    @Test
    public void shouldReplayFullDataFromFile()
    {
        final long length = 4 * FRAME_LENGTH;
        final long correlationId = 1L;

        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession,
            null,
            null))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);

            replaySession.doWork();
            assertEquals(replaySession.state(), ReplaySession.State.INIT);

            when(mockReplayPub.isConnected()).thenReturn(true);

            final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
            mockPublication(mockReplayPub, termBuffer);

            assertNotEquals(0, replaySession.doWork());
            assertThat(messageCounter, is(2));

            validateFrame(termBuffer, 0, FRAME_LENGTH, 0, UNFRAGMENTED, 0, 0);
            validateFrame(termBuffer, FRAME_LENGTH, FRAME_LENGTH, 1, BEGIN_FRAG_FLAG, 0, 0);
            validateFrame(termBuffer, 2 * FRAME_LENGTH, FRAME_LENGTH, 2, END_FRAG_FLAG, 0, 0);

            verify(mockReplayPub).appendPadding(FRAME_LENGTH - HEADER_LENGTH);
            assertTrue(replaySession.isDone());
        }
    }

    @Test
    public void shouldGiveUpIfPublishersAreNotConnectedAfterTimeout()
    {
        final long length = 1024L;
        final long correlationId = 1L;
        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession,
            recordingPositionCounter,
            null))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);

            replaySession.doWork();

            epochClock.update(CONNECT_TIMEOUT_MS + TIME + 1L);
            replaySession.doWork();
            assertTrue(replaySession.isDone());
        }
    }

    @Test
    public void shouldReplayFromActiveRecording() throws IOException
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));

        final int recordingId = RECORDING_ID + 1;
        recordingSummary.recordingId = recordingId;
        recordingSummary.stopPosition = NULL_POSITION;

        when(mockCatalog.stopPosition(recordingId)).thenReturn(START_POSITION + FRAME_LENGTH * 4);
        recordingPosition = START_POSITION;

        context.recordChecksumBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH)));
        final RecordingWriter writer = new RecordingWriter(
            recordingId, START_POSITION, SEGMENT_LENGTH, mockImage, context);

        writer.init();

        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(TERM_BUFFER_LENGTH, 64));

        final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
        final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
        header.buffer(buffer);

        recordFragment(writer, buffer, headerFwt, header, 0, FRAME_LENGTH, 0, UNFRAGMENTED,
            HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, FRAME_LENGTH, FRAME_LENGTH, 1, BEGIN_FRAG_FLAG,
            HDR_TYPE_DATA, SESSION_ID);

        final long length = 5 * FRAME_LENGTH;
        final long correlationId = 1L;
        final int sessionId = 42;
        final int streamId = 21;

        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession,
            recordingPositionCounter,
            null))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);
            when(mockReplayPub.sessionId()).thenReturn(sessionId);
            when(mockReplayPub.streamId()).thenReturn(streamId);

            replaySession.doWork();

            assertEquals(replaySession.state(), ReplaySession.State.INIT);

            when(mockReplayPub.isConnected()).thenReturn(true);

            mockPublication(mockReplayPub, termBuffer);

            assertNotEquals(0, replaySession.doWork());
            assertThat(messageCounter, is(1));

            validateFrame(termBuffer, 0, FRAME_LENGTH, 0, UNFRAGMENTED, sessionId, streamId);
            validateFrame(termBuffer, FRAME_LENGTH, FRAME_LENGTH, 1, BEGIN_FRAG_FLAG, sessionId, streamId);

            assertEquals(0, replaySession.doWork());

            recordFragment(writer, buffer, headerFwt, header, FRAME_LENGTH * 2, FRAME_LENGTH, 2,
                END_FRAG_FLAG, HDR_TYPE_DATA, SESSION_ID);
            recordFragment(writer, buffer, headerFwt, header, FRAME_LENGTH * 3, FRAME_LENGTH, 3,
                UNFRAGMENTED, HDR_TYPE_PAD, SESSION_ID);

            writer.close();

            when(recordingPositionCounter.isClosed()).thenReturn(true);
            when(mockCatalog.stopPosition(recordingId)).thenReturn(START_POSITION + FRAME_LENGTH * 4);
            assertNotEquals(0, replaySession.doWork());

            validateFrame(termBuffer, 2 * FRAME_LENGTH, FRAME_LENGTH, 2, END_FRAG_FLAG, sessionId, streamId);
            verify(mockReplayPub).appendPadding(FRAME_LENGTH - HEADER_LENGTH);

            assertTrue(replaySession.isDone());
        }
    }

    @Test
    public void shouldThrowArchiveExceptionIfCrcFails()
    {
        final long length = 4 * FRAME_LENGTH;
        final long correlationId = 1L;

        final Checksum checksum = crc32();
        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION + 2 * FRAME_LENGTH,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession,
            null,
            checksum))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);

            replaySession.doWork();
            assertEquals(ReplaySession.State.INIT, replaySession.state());

            when(mockReplayPub.isConnected()).thenReturn(true);

            final ArchiveException exception = assertThrows(ArchiveException.class, replaySession::doWork);
            assertEquals(ArchiveException.GENERIC, exception.errorCode());
            assertThat(exception.getMessage(), Matchers.startsWith("ERROR - CRC checksum mismatch at offset=0"));
            verify(mockReplayPub, never()).tryClaim(anyInt(), any(BufferClaim.class));
        }
    }

    @Test
    public void shouldDoCrcForEachDataFrame() throws IOException
    {
        context.recordChecksum(crc32());
        context.recordChecksumBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH)));
        final RecordingWriter writer = new RecordingWriter(
            RECORDING_ID, START_POSITION, SEGMENT_LENGTH, mockImage, context);

        writer.init();

        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(TERM_BUFFER_LENGTH, 64));
        final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
        final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
        header.buffer(buffer);

        recordFragment(writer, buffer, headerFwt, header, 0, FRAME_LENGTH, 10, UNFRAGMENTED,
            HDR_TYPE_DATA, checksum(FRAME_LENGTH, 10));
        recordFragment(writer, buffer, headerFwt, header, FRAME_LENGTH, FRAME_LENGTH, 20, BEGIN_FRAG_FLAG,
            HDR_TYPE_DATA, checksum(FRAME_LENGTH, 20));
        recordFragment(writer, buffer, headerFwt, header, FRAME_LENGTH * 2, FRAME_LENGTH, 30, END_FRAG_FLAG,
            HDR_TYPE_DATA, checksum(FRAME_LENGTH, 30));
        recordFragment(writer, buffer, headerFwt, header, FRAME_LENGTH * 3, FRAME_LENGTH, 40, UNFRAGMENTED,
            HDR_TYPE_PAD, SESSION_ID);

        writer.close();

        final long length = 4 * FRAME_LENGTH;
        final long correlationId = 1L;
        final int sessionId = Integer.MIN_VALUE;
        final int streamId = Integer.MAX_VALUE;

        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            length,
            correlationId,
            mockReplayPub,
            mockControlSession,
            null,
            context.recordChecksum()))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);
            when(mockReplayPub.sessionId()).thenReturn(sessionId);
            when(mockReplayPub.streamId()).thenReturn(streamId);

            replaySession.doWork();
            assertEquals(replaySession.state(), ReplaySession.State.INIT);

            when(mockReplayPub.isConnected()).thenReturn(true);

            final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
            mockPublication(mockReplayPub, termBuffer);

            assertNotEquals(0, replaySession.doWork());
            assertThat(messageCounter, is(2));

            validateFrame(termBuffer, 0, FRAME_LENGTH, 10, UNFRAGMENTED, sessionId, streamId);
            validateFrame(termBuffer, FRAME_LENGTH, FRAME_LENGTH, 20, BEGIN_FRAG_FLAG, sessionId, streamId);
            validateFrame(termBuffer, 2 * FRAME_LENGTH, FRAME_LENGTH, 30, END_FRAG_FLAG, sessionId, streamId);

            verify(mockReplayPub).appendPadding(FRAME_LENGTH - HEADER_LENGTH);
            assertTrue(replaySession.isDone());
        }
    }

    @Test
    public void shouldNotWritePaddingIfOfferBlockFails()
    {
        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            4 * FRAME_LENGTH,
            1L,
            mockReplayPub,
            mockControlSession,
            null,
            null))
        {
            when(mockReplayPub.isConnected()).thenReturn(true);
            when(mockReplayPub.offerBlock(any(MutableDirectBuffer.class), anyInt(), anyInt()))
                .thenReturn(BACK_PRESSURED);

            assertEquals(1, replaySession.doWork());

            verify(mockReplayPub).offerBlock(any(MutableDirectBuffer.class), anyInt(), anyInt());
            verify(mockReplayPub, never()).appendPadding(anyInt());
        }
    }

    @Test
    public void shouldNotWritePaddingIfReplayLimitReached()
    {
        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            1500,
            1L,
            mockReplayPub,
            mockControlSession,
            null,
            null))
        {
            when(mockReplayPub.isConnected()).thenReturn(true);
            final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
            mockPublication(mockReplayPub, termBuffer);

            assertEquals(2, replaySession.doWork());

            validateFrame(termBuffer, 0, FRAME_LENGTH, 0, UNFRAGMENTED, 0, 0);
            validateFrame(termBuffer, FRAME_LENGTH, FRAME_LENGTH, 1, BEGIN_FRAG_FLAG, 0, 0);
            validateFrame(termBuffer, FRAME_LENGTH * 2, 0, 0, (byte)0, 0, 0);
            verify(mockReplayPub).offerBlock(any(MutableDirectBuffer.class), eq(0), eq(2 * FRAME_LENGTH));
            verify(mockReplayPub, never()).appendPadding(anyInt());
            assertTrue(replaySession.isDone());
        }
    }

    @Test
    public void shouldCalculateBlockSizeBasedOnFullFragments() throws IOException
    {
        context.recordChecksumBuffer(new UnsafeBuffer(ByteBuffer.allocateDirect(TERM_BUFFER_LENGTH)));
        final RecordingWriter writer = new RecordingWriter(
            RECORDING_ID, START_POSITION, SEGMENT_LENGTH, mockImage, context);

        writer.init();

        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(TERM_BUFFER_LENGTH, 64));
        final DataHeaderFlyweight headerFwt = new DataHeaderFlyweight();
        final Header header = new Header(INITIAL_TERM_ID, Integer.numberOfLeadingZeros(TERM_BUFFER_LENGTH));
        header.buffer(buffer);

        recordFragment(writer, buffer, headerFwt, header, 0, 100, 11, UNFRAGMENTED, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, 128, 200, 22, UNFRAGMENTED, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, 352, 300, 33, UNFRAGMENTED, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, 672, 400, 44, UNFRAGMENTED, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, 1088, 500, 55, UNFRAGMENTED, HDR_TYPE_DATA, SESSION_ID);
        recordFragment(writer, buffer, headerFwt, header, 1600, 6000, 66, UNFRAGMENTED, HDR_TYPE_DATA, SESSION_ID);

        writer.close();

        final int sessionId = 555;
        final int streamId = 777;

        try (ReplaySession replaySession = replaySession(
            RECORDING_POSITION,
            1_000_000,
            -1,
            mockReplayPub,
            mockControlSession,
            null,
            null))
        {
            when(mockReplayPub.isClosed()).thenReturn(false);
            when(mockReplayPub.isConnected()).thenReturn(false);
            when(mockReplayPub.sessionId()).thenReturn(sessionId);
            when(mockReplayPub.streamId()).thenReturn(streamId);

            replaySession.doWork();
            assertEquals(replaySession.state(), ReplaySession.State.INIT);

            when(mockReplayPub.isConnected()).thenReturn(true);

            final UnsafeBuffer termBuffer = new UnsafeBuffer(allocateDirectAligned(4096, 64));
            mockPublication(mockReplayPub, termBuffer);

            assertEquals(2, replaySession.doWork());
            assertThat(messageCounter, is(1));

            validateFrame(termBuffer, 0, 100, 11, UNFRAGMENTED, sessionId, streamId);
            validateFrame(termBuffer, 128, 200, 22, UNFRAGMENTED, sessionId, streamId);
            validateFrame(termBuffer, 352, 300, 33, UNFRAGMENTED, sessionId, streamId);
            validateFrame(termBuffer, 672, 400, 44, UNFRAGMENTED, sessionId, streamId);
            validateFrame(termBuffer, 1088, 500, 55, UNFRAGMENTED, sessionId, streamId);

            verify(mockReplayPub, never()).appendPadding(anyInt());
        }

    }

    private void recordFragment(
        final RecordingWriter recordingWriter,
        final UnsafeBuffer buffer,
        final DataHeaderFlyweight headerFlyweight,
        final Header header,
        final int offset,
        final int frameLength,
        final int message,
        final byte flags,
        final int type,
        final int sessionId)
    {
        headerFlyweight.wrap(buffer, offset, HEADER_LENGTH);
        headerFlyweight
            .streamId(STREAM_ID)
            .sessionId(sessionId)
            .termOffset(offset)
            .termId(INITIAL_TERM_ID)
            .reservedValue(message)
            .headerType(type)
            .flags(flags)
            .frameLength(frameLength);

        buffer.setMemory(
            offset + HEADER_LENGTH,
            frameLength - HEADER_LENGTH,
            (byte)message);

        header.offset(offset);

        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        recordingWriter.onBlock(buffer, offset, alignedLength, SESSION_ID, INITIAL_TERM_ID);
        recordingPosition += alignedLength;
    }

    private void mockPublication(final ExclusivePublication replay, final UnsafeBuffer termBuffer)
    {
        when(replay.offerBlock(any(MutableDirectBuffer.class), anyInt(), anyInt())).then(
            (invocation) ->
            {
                final MutableDirectBuffer buffer = invocation.getArgument(0);
                final int offset = invocation.getArgument(1);
                final int length = invocation.getArgument(2);
                termBuffer.putBytes(offerBlockOffset, buffer, offset, length);
                messageCounter++;
                offerBlockOffset += length;
                return (long)length;
            });

        when(replay.appendPadding(anyInt())).then(
            (invocation) ->
            {
                final int claimedSize = invocation.getArgument(0);
                messageCounter++;

                return (long)claimedSize;
            });
    }

    private ReplaySession replaySession(
        final long position,
        final long length,
        final long correlationId,
        final ExclusivePublication replay,
        final ControlSession controlSession,
        final Counter recordingPositionCounter,
        final Checksum checksum)
    {
        return new ReplaySession(
            position,
            length,
            REPLAY_ID,
            CONNECT_TIMEOUT_MS,
            correlationId,
            controlSession,
            proxy,
            replayBuffer,
            mockCatalog,
            archiveDir,
            epochClock,
            replay,
            recordingSummary,
            recordingPositionCounter,
            checksum);
    }

    static void validateFrame(
        final UnsafeBuffer buffer,
        final int offset,
        final int frameLength,
        final int message,
        final byte flags,
        final int sessionId,
        final int streamId)
    {

        assertEquals(frameLength, frameLength(buffer, offset));
        assertEquals(flags, frameFlags(buffer, offset));
        assertEquals(sessionId, buffer.getInt(offset + SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(streamId, buffer.getInt(offset + STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN));
        assertEquals(message, buffer.getLong(offset + RESERVED_VALUE_OFFSET));
        assertEquals(message, buffer.getByte(offset + HEADER_LENGTH));
    }

    private int checksum(final int frameLength, final int message)
    {
        final byte[] data = new byte[frameLength - HEADER_LENGTH];
        fill(data, (byte)message);
        final ByteBuffer buffer = allocateDirectAligned(data.length, CACHE_LINE_LENGTH);
        final long address = address(buffer);
        buffer.put(data).flip();

        return crc32().compute(address, 0, data.length);
    }
}
