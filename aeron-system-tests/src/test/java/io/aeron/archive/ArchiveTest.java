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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveProxy;
import io.aeron.archive.client.ControlResponseAdapter;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.*;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static io.aeron.archive.ArchiveTests.awaitConnectedReply;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ArchiveTest
{
    private static Stream<Arguments> threadingModes()
    {
        return Stream.of(
            arguments(ThreadingMode.INVOKER, ArchiveThreadingMode.SHARED),
            arguments(ThreadingMode.SHARED, ArchiveThreadingMode.SHARED),
            arguments(ThreadingMode.DEDICATED, ArchiveThreadingMode.DEDICATED));
    }

    private static final String CONTROL_RESPONSE_URI = CommonContext.IPC_CHANNEL;
    private static final int CONTROL_RESPONSE_STREAM_ID = 100;
    private static final String REPLAY_URI = CommonContext.IPC_CHANNEL;
    private static final int MESSAGE_COUNT = 5000;
    private static final int SYNC_LEVEL = 0;
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final int REPLAY_STREAM_ID = 101;

    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(4096, FrameDescriptor.FRAME_ALIGNMENT));
    private final Random rnd = new Random();
    private final long seed = System.nanoTime();

    @RegisterExtension
    public final TestWatcher randomSeedWatcher = ArchiveTests.newWatcher(seed);

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private long controlSessionId;
    private String publishUri;
    private Aeron client;
    private Archive archive;
    private TestMediaDriver driver;
    private long recordingId;
    private long remaining;
    private int messageCount;
    private int[] messageLengths;
    private long totalDataLength;
    private long totalRecordingLength;
    private volatile long recorded = 0;
    private long requestedStartPosition;
    private volatile long stopPosition = NULL_POSITION;
    private Throwable trackerError;

    private Subscription controlResponse;
    private long correlationId;
    private long startPosition;
    private int requestedInitialTermId;

    private Thread replayConsumer = null;
    private Thread progressTracker = null;

    private void before(final ThreadingMode threadingMode, final ArchiveThreadingMode archiveThreadingMode)
    {
        if (threadingMode == ThreadingMode.INVOKER)
        {
            TestMediaDriver.notSupportedOnCMediaDriver("C driver does not integrate with Java Invoker");
        }

        IoUtil.delete(new File(CommonContext.getAeronDirectoryName()), false);
        rnd.setSeed(seed);
        requestedInitialTermId = rnd.nextInt(1234);

        final int termLength = 1 << (16 + rnd.nextInt(10)); // 1M to 8M
        final int mtu = 1 << (10 + rnd.nextInt(3)); // 1024 to 8096
        final int requestedStartTermOffset = BitUtil.align(rnd.nextInt(termLength), FrameDescriptor.FRAME_ALIGNMENT);
        final int requestedStartTermId = requestedInitialTermId + rnd.nextInt(1000);
        final int segmentFileLength = termLength << rnd.nextInt(4);

        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .termBufferSparseFile(true)
                .threadingMode(threadingMode)
                .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
                .spiesSimulateConnection(true)
                .errorHandler(Tests::onError)
                .dirDeleteOnStart(true),
            testWatcher);

        final Archive.Context archiveContext = new Archive.Context()
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .fileSyncLevel(SYNC_LEVEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive-test"))
            .segmentFileLength(segmentFileLength)
            .threadingMode(archiveThreadingMode)
            .idleStrategySupplier(YieldingIdleStrategy::new)
            .errorHandler(Tests::onError);

        if (threadingMode == ThreadingMode.INVOKER)
        {
            archiveContext.mediaDriverAgentInvoker(driver.sharedAgentInvoker());
        }

        archive = Archive.launch(archiveContext);
        client = Aeron.connect();

        requestedStartPosition =
            ((requestedStartTermId - requestedInitialTermId) * (long)termLength) + requestedStartTermOffset;

        publishUri = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:24325")
            .termLength(termLength)
            .mtu(mtu)
            .initialTermId(requestedInitialTermId)
            .termId(requestedStartTermId)
            .termOffset(requestedStartTermOffset)
            .build();
    }

    @AfterEach
    public void after() throws Exception
    {
        try
        {
            if (null != replayConsumer)
            {
                replayConsumer.interrupt();
                replayConsumer.join();
            }

            if (null != progressTracker)
            {
                progressTracker.interrupt();
                progressTracker.join();
            }
        }
        finally
        {
            CloseHelper.closeAll(client, archive, driver);

            if (null != archive)
            {
                archive.context().deleteDirectory();
            }

            if (null != driver)
            {
                driver.context().deleteDirectory();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("threadingModes")
    @Timeout(10)
    public void recordAndReplayExclusivePublication(
        final ThreadingMode threadingMode, final ArchiveThreadingMode archiveThreadingMode)
    {
        before(threadingMode, archiveThreadingMode);

        final String controlChannel = archive.context().localControlChannel();
        final int controlStreamId = archive.context().localControlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        final Publication controlPublication = client.addPublication(controlChannel, controlStreamId);
        final Subscription recordingEvents = client.addSubscription(recordingChannel, recordingStreamId);
        final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

        prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

        final ExclusivePublication recordedPublication = client.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

        final int sessionId = recordedPublication.sessionId();
        final int termBufferLength = recordedPublication.termBufferLength();
        final int initialTermId = recordedPublication.initialTermId();
        final int maxPayloadLength = recordedPublication.maxPayloadLength();
        final long startPosition = recordedPublication.position();

        assertEquals(requestedStartPosition, startPosition);
        assertEquals(requestedInitialTermId, recordedPublication.initialTermId());
        preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, startPosition);

        prepAndSendMessages(recordingEvents, recordedPublication);

        postPublicationValidations(
            archiveProxy, recordingEvents, termBufferLength, initialTermId, maxPayloadLength);
    }

    @ParameterizedTest
    @MethodSource("threadingModes")
    @Timeout(10)
    public void replayExclusivePublicationWhileRecording(
        final ThreadingMode threadingMode, final ArchiveThreadingMode archiveThreadingMode)
    {
        before(threadingMode, archiveThreadingMode);

        final String controlChannel = archive.context().localControlChannel();
        final int controlStreamId = archive.context().localControlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        final Publication controlPublication = client.addPublication(controlChannel, controlStreamId);
        final Subscription recordingEvents = client.addSubscription(recordingChannel, recordingStreamId);
        final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

        prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

        final ExclusivePublication recordedPublication = client.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

        final int sessionId = recordedPublication.sessionId();
        final int termBufferLength = recordedPublication.termBufferLength();
        final int initialTermId = recordedPublication.initialTermId();
        final int maxPayloadLength = recordedPublication.maxPayloadLength();
        final long startPosition = recordedPublication.position();

        assertEquals(requestedStartPosition, startPosition);
        assertEquals(requestedInitialTermId, recordedPublication.initialTermId());
        preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, startPosition);

        final CountDownLatch latch = new CountDownLatch(2);

        prepMessagesAndListener(recordingEvents, latch);
        replayConsumer = validateActiveRecordingReplay(
            archiveProxy, termBufferLength, initialTermId, maxPayloadLength, latch);

        publishDataToBeRecorded(recordedPublication);
        await(latch);
    }

    @ParameterizedTest
    @MethodSource("threadingModes")
    @Timeout(10)
    public void recordAndReplayConcurrentPublication(
        final ThreadingMode threadingMode, final ArchiveThreadingMode archiveThreadingMode)
    {
        before(threadingMode, archiveThreadingMode);

        final Publication controlPublication = client.addPublication(
            archive.context().localControlChannel(), archive.context().localControlStreamId());
        final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

        final Subscription recordingEvents = client.addSubscription(
            archive.context().recordingEventsChannel(), archive.context().recordingEventsStreamId());
        prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

        final Publication recordedPublication = client.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);
        final int termBufferLength = recordedPublication.termBufferLength();
        final long startPosition = recordedPublication.position();

        preSendChecks(archiveProxy, recordingEvents, recordedPublication.sessionId(), termBufferLength, startPosition);
        prepAndSendMessages(recordingEvents, recordedPublication);

        postPublicationValidations(
            archiveProxy,
            recordingEvents,
            termBufferLength,
            recordedPublication.initialTermId(),
            recordedPublication.maxPayloadLength());
    }

    private void preSendChecks(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int sessionId,
        final int termBufferLength,
        final long startPosition)
    {
        final MutableBoolean isRecordingStarted = new MutableBoolean();
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onStart(
                    final long recordingId,
                    final long startPosition0,
                    final int sessionId0,
                    final int streamId,
                    final String channel,
                    final String sourceIdentity)
                {
                    ArchiveTest.this.recordingId = recordingId;
                    assertEquals(PUBLISH_STREAM_ID, streamId);
                    assertEquals(sessionId, sessionId0);
                    assertEquals(startPosition, startPosition0);
                    isRecordingStarted.set(true);
                }
            },
            recordingEvents,
            1);

        while (!isRecordingStarted.get())
        {
            if (recordingEventsAdapter.poll() == 0)
            {
                if (!recordingEvents.isConnected())
                {
                    throw new IllegalStateException("recording events not connected");
                }

                Tests.yield();
            }
        }

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
    }

    private void postPublicationValidations(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int termBufferLength,
        final int initialTermId,
        final int maxPayloadLength)
    {
        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
        assertNull(trackerError);

        final long requestCorrelationId = correlationId++;
        if (!archiveProxy.stopRecording(publishUri, PUBLISH_STREAM_ID, requestCorrelationId, controlSessionId))
        {
            throw new IllegalStateException("failed to send stop recording");
        }

        ArchiveTests.awaitOk(controlResponse, requestCorrelationId);

        final MutableBoolean isRecordingStopped = new MutableBoolean();
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onStop(final long id, final long startPosition, final long stopPosition)
                {
                    assertEquals(recordingId, id);
                    isRecordingStopped.set(true);
                }
            },
            recordingEvents,
            1);

        while (!isRecordingStopped.get())
        {
            if (recordingEventsAdapter.poll() == 0)
            {
                Tests.yield();
            }
        }

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
        validateArchiveFile(recordingId);
        validateReplay(archiveProxy, initialTermId, maxPayloadLength, termBufferLength);
    }

    private void prePublicationActionsAndVerifications(
        final ArchiveProxy archiveProxy, final Publication controlPublication, final Subscription recordingEvents)
    {
        Tests.awaitConnected(controlPublication);
        Tests.awaitConnected(recordingEvents);

        controlResponse = client.addSubscription(CONTROL_RESPONSE_URI, CONTROL_RESPONSE_STREAM_ID);
        final long connectCorrelationId = correlationId++;
        assertTrue(archiveProxy.connect(CONTROL_RESPONSE_URI, CONTROL_RESPONSE_STREAM_ID, connectCorrelationId));

        awaitConnectedReply(controlResponse, connectCorrelationId, (sessionId) -> controlSessionId = sessionId);
        verifyEmptyDescriptorList(archiveProxy);

        final long startRecordingCorrelationId = correlationId++;
        if (!archiveProxy.startRecording(
            publishUri,
            PUBLISH_STREAM_ID,
            SourceLocation.LOCAL,
            startRecordingCorrelationId,
            controlSessionId))
        {
            throw new IllegalStateException("failed to start recording");
        }

        ArchiveTests.awaitOk(controlResponse, startRecordingCorrelationId);
    }

    private void verifyEmptyDescriptorList(final ArchiveProxy client)
    {
        final long requestCorrelationId = correlationId++;
        client.listRecordings(0, 100, requestCorrelationId, controlSessionId);
        ArchiveTests.awaitResponse(controlResponse, requestCorrelationId);
    }

    private void verifyDescriptorListOngoingArchive(
        final ArchiveProxy archiveProxy, final int publicationTermBufferLength)
    {
        final long requestCorrelationId = correlationId++;
        archiveProxy.listRecording(recordingId, requestCorrelationId, controlSessionId);
        final MutableBoolean isDone = new MutableBoolean();

        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                public void onRecordingDescriptor(
                    final long controlSessionId,
                    final long correlationId,
                    final long recordingId,
                    final long startTimestamp,
                    final long stopTimestamp,
                    final long startPosition,
                    final long stopPosition,
                    final int initialTermId,
                    final int segmentFileLength,
                    final int termBufferLength,
                    final int mtuLength,
                    final int sessionId,
                    final int streamId,
                    final String strippedChannel,
                    final String originalChannel,
                    final String sourceIdentity)
                {
                    assertEquals(requestCorrelationId, correlationId);
                    assertEquals(ArchiveTest.this.recordingId, recordingId);
                    assertEquals(publicationTermBufferLength, termBufferLength);
                    assertEquals(PUBLISH_STREAM_ID, streamId);
                    assertEquals(publishUri, originalChannel);

                    isDone.set(true);
                }
            },
            controlResponse,
            1);

        while (!isDone.get())
        {
            if (controlResponseAdapter.poll() == 0)
            {
                if (!controlResponse.isConnected())
                {
                    throw new IllegalStateException("control response not connected");
                }

                Tests.yield();
            }
        }
    }

    private void prepAndSendMessages(final Subscription recordingEvents, final Publication publication)
    {
        final CountDownLatch complete = new CountDownLatch(1);
        prepMessagesAndListener(recordingEvents, complete);
        publishDataToBeRecorded(publication);
        await(complete);
    }

    private void prepAndSendMessages(final Subscription recordingEvents, final ExclusivePublication publication)
    {
        final CountDownLatch complete = new CountDownLatch(1);
        prepMessagesAndListener(recordingEvents, complete);
        publishDataToBeRecorded(publication);
        await(complete);
    }

    private void await(final CountDownLatch latch)
    {
        try
        {
            latch.await();
        }
        catch (final InterruptedException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void prepMessagesAndListener(final Subscription recordingEvents, final CountDownLatch latch)
    {
        messageLengths = new int[MESSAGE_COUNT];
        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - HEADER_LENGTH;
            messageLengths[i] = messageLength + HEADER_LENGTH;
            totalDataLength += BitUtil.align(messageLengths[i], FrameDescriptor.FRAME_ALIGNMENT);
        }

        progressTracker = trackRecordingProgress(recordingEvents, latch);
    }

    private void publishDataToBeRecorded(final Publication publication)
    {
        startPosition = publication.position();

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < MESSAGE_COUNT; i++)
        {
            final int dataLength = messageLengths[i] - HEADER_LENGTH;
            buffer.putInt(0, i);

            while (true)
            {
                final long result = publication.offer(buffer, 0, dataLength);
                if (result > 0)
                {
                    break;
                }

                if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
                {
                    throw new IllegalStateException("Publication not connected: result=" + result);
                }

                Tests.yield();
            }
        }

        final long position = publication.position();
        totalRecordingLength = position - startPosition;
        stopPosition = position;
    }

    private void validateReplay(
        final ArchiveProxy archiveProxy,
        final int initialTermId,
        final int maxPayloadLength,
        final int termBufferLength)
    {
        try (Subscription replay = client.addSubscription(REPLAY_URI, REPLAY_STREAM_ID))
        {
            final long replayCorrelationId = correlationId++;

            if (!archiveProxy.replay(
                recordingId,
                startPosition,
                totalRecordingLength,
                REPLAY_URI,
                REPLAY_STREAM_ID,
                replayCorrelationId,
                controlSessionId))
            {
                throw new IllegalStateException("failed to replay");
            }

            ArchiveTests.awaitOk(controlResponse, replayCorrelationId);
            Tests.awaitConnected(replay);

            final Image image = replay.images().get(0);
            assertEquals(initialTermId, image.initialTermId());
            assertEquals(maxPayloadLength + HEADER_LENGTH, image.mtuLength());
            assertEquals(termBufferLength, image.termBufferLength());
            assertEquals(startPosition, image.position());

            messageCount = 0;
            remaining = totalDataLength;

            while (remaining > 0)
            {
                final int fragments = replay.poll(this::validateFragment, 10);
                if (0 == fragments)
                {
                    Tests.yield();
                }
            }

            assertEquals(MESSAGE_COUNT, messageCount);
            assertEquals(0L, remaining);
        }
    }

    private void validateArchiveFile(final long recordingId)
    {
        final File archiveDir = archive.context().archiveDir();
        final Catalog catalog = archive.context().catalog();
        remaining = totalDataLength;
        this.messageCount = 0;

        while (catalog.stopPosition(recordingId) != stopPosition)
        {
            Tests.yield();
        }

        try (RecordingReader recordingReader = new RecordingReader(
            catalog.recordingSummary(recordingId, new RecordingSummary()),
            archiveDir,
            NULL_POSITION,
            AeronArchive.NULL_LENGTH))
        {
            while (!recordingReader.isDone())
            {
                if (0 == recordingReader.poll(this::validateRecordingFragment, MESSAGE_COUNT))
                {
                    Tests.yield();
                }
            }
        }

        assertEquals(0L, remaining);
        assertEquals(MESSAGE_COUNT, messageCount);
    }

    private void validateRecordingFragment(
        final UnsafeBuffer buffer,
        final int offset,
        final int length,
        final int frameType,
        final byte flags,
        final long reservedValue)
    {
        if (!FrameDescriptor.isPaddingFrame(buffer, offset - HEADER_LENGTH))
        {
            final int expectedLength = messageLengths[messageCount] - HEADER_LENGTH;
            if (length != expectedLength)
            {
                fail("messageLength=" + length + " expected=" + expectedLength + " messageCount=" + messageCount);
            }

            assertEquals(messageCount, buffer.getInt(offset));
            assertEquals((byte)'z', buffer.getByte(offset + 4));

            remaining -= BitUtil.align(messageLengths[messageCount], FrameDescriptor.FRAME_ALIGNMENT);
            messageCount++;
        }
    }

    private void validateFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        assertEquals(messageLengths[messageCount] - HEADER_LENGTH, length);
        assertEquals(messageCount, buffer.getInt(offset));
        assertEquals((byte)'z', buffer.getByte(offset + 4));

        remaining -= BitUtil.align(messageLengths[messageCount], FrameDescriptor.FRAME_ALIGNMENT);
        messageCount++;
    }

    private Thread trackRecordingProgress(final Subscription recordingEvents, final CountDownLatch latch)
    {
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onProgress(final long recordingId0, final long startPosition, final long position)
                {
                    assertEquals(recordingId, recordingId0);
                    recorded = position - startPosition;
                }
            },
            recordingEvents,
            1);

        final Thread thread = new Thread(
            () ->
            {
                try
                {
                    recorded = 0;

                    while (stopPosition == NULL_POSITION || recorded < totalRecordingLength)
                    {
                        if (recordingEventsAdapter.poll() == 0)
                        {
                            if (!recordingEvents.isConnected())
                            {
                                break;
                            }

                            Tests.sleep(1);
                        }
                    }
                }
                catch (final Throwable throwable)
                {
                    throwable.printStackTrace();
                    trackerError = throwable;
                }

                latch.countDown();
            });

        thread.setDaemon(true);
        thread.setName("recording-progress-tracker");
        thread.start();

        return thread;
    }

    private Thread validateActiveRecordingReplay(
        final ArchiveProxy archiveProxy,
        final int termBufferLength,
        final int initialTermId,
        final int maxPayloadLength,
        final CountDownLatch completeLatch)
    {
        final Thread thread = new Thread(
            () ->
            {
                while (0 == recorded)
                {
                    Tests.sleep(1);
                }

                try (Subscription replay = client.addSubscription(REPLAY_URI, REPLAY_STREAM_ID))
                {
                    final long replayCorrelationId = correlationId++;

                    if (!archiveProxy.replay(
                        recordingId,
                        startPosition,
                        Long.MAX_VALUE,
                        REPLAY_URI,
                        REPLAY_STREAM_ID,
                        replayCorrelationId,
                        controlSessionId))
                    {
                        throw new IllegalStateException("failed to start replay");
                    }

                    ArchiveTests.awaitOk(controlResponse, replayCorrelationId);
                    Tests.awaitConnected(replay);

                    final Image image = replay.images().get(0);
                    assertEquals(initialTermId, image.initialTermId());
                    assertEquals(maxPayloadLength + HEADER_LENGTH, image.mtuLength());
                    assertEquals(termBufferLength, image.termBufferLength());
                    assertEquals(startPosition, image.position());

                    messageCount = 0;
                    remaining = totalDataLength;

                    final FragmentHandler fragmentHandler = this::validateFragment;
                    while (messageCount < MESSAGE_COUNT)
                    {
                        final int fragments = replay.poll(fragmentHandler, 10);
                        if (0 == fragments)
                        {
                            if (!replay.isConnected())
                            {
                                break;
                            }

                            Tests.yield();
                        }
                    }
                }

                completeLatch.countDown();
            });

        thread.setName("replay-consumer");
        thread.setDaemon(true);
        thread.start();

        return thread;
    }
}
