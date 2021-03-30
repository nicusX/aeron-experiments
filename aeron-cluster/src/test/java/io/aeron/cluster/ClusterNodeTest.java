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
package io.aeron.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterNodeTest
{
    private static final long CATALOG_CAPACITY = 1024 * 1024;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    @BeforeEach
    public void before()
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(ClusterTests.errorHandler(0))
                .dirDeleteOnStart(true),
            new Archive.Context()
                .catalogCapacity(CATALOG_CAPACITY)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(0))
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .logChannel("aeron:ipc")
                .deleteDirOnStart(true));
    }

    @AfterEach
    public void after()
    {
        final ConsensusModule consensusModule = null == clusteredMediaDriver ?
            null : clusteredMediaDriver.consensusModule();

        CloseHelper.closeAll(aeronCluster, consensusModule, container, clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteDirectory();
            clusteredMediaDriver.mediaDriver().context().deleteDirectory();
            container.context().deleteDirectory();
        }
    }

    @Test
    @Timeout(10)
    public void shouldConnectAndSendKeepAlive()
    {
        container = launchEchoService();
        aeronCluster = connectToCluster(null);

        assertTrue(aeronCluster.sendKeepAlive());
    }

    @Test
    @Timeout(10)
    public void shouldEchoMessageViaServiceUsingDirectOffer()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertEquals(msg, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchEchoService();
        aeronCluster = connectToCluster(listener);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @Timeout(10)
    public void shouldEchoMessageViaServiceUsingTryClaim()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertEquals(msg, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchEchoService();
        aeronCluster = connectToCluster(listener);

        final BufferClaim bufferClaim = new BufferClaim();
        long publicationResult;
        do
        {
            publicationResult = aeronCluster.tryClaim(msg.length(), bufferClaim);
            if (publicationResult > 0)
            {
                final int offset = bufferClaim.offset() + AeronCluster.SESSION_HEADER_LENGTH;
                bufferClaim.buffer().putBytes(offset, msgBuffer, 0, msg.length());
                bufferClaim.commit();
            }
            else
            {
                Tests.yield();
            }
        }
        while (publicationResult < 0);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @Timeout(10)
    public void shouldScheduleEventInService()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                final String expected = msg + "-scheduled";
                assertEquals(expected, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchTimedService();
        aeronCluster = connectToCluster(listener);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    @Test
    @Timeout(10)
    public void shouldSendResponseAfterServiceMessage()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final String msg = "Hello World!";
        msgBuffer.putStringWithoutLengthAscii(0, msg);

        final MutableInteger messageCount = new MutableInteger();

        final EgressListener listener =
            (clusterSessionId, timestamp, buffer, offset, length, header) ->
            {
                assertEquals(msg, buffer.getStringWithoutLengthAscii(offset, length));
                messageCount.value += 1;
            };

        container = launchServiceMessageIngressService();
        aeronCluster = connectToCluster(listener);

        offerMessage(msgBuffer, msg);
        awaitResponse(messageCount);

        ClusterTests.failOnClusterError();
    }

    private void offerMessage(final ExpandableArrayBuffer msgBuffer, final String msg)
    {
        while (aeronCluster.offer(msgBuffer, 0, msg.length()) < 0)
        {
            Tests.yield();
        }
    }

    private void awaitResponse(final MutableInteger messageCount)
    {
        while (messageCount.get() == 0)
        {
            if (aeronCluster.pollEgress() <= 0)
            {
                Tests.yield();
            }
        }
    }

    private ClusteredServiceContainer launchEchoService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                idleStrategy.reset();
                while (session.offer(buffer, offset, length) < 0)
                {
                    idleStrategy.idle();
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .errorHandler(Tests::onError));
    }

    private ClusteredServiceContainer launchTimedService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
            private long clusterSessionId;
            private int nextCorrelationId;
            private String msg;

            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                clusterSessionId = session.id();
                msg = buffer.getStringWithoutLengthAscii(offset, length);
                final long correlationId = serviceCorrelationId(nextCorrelationId++);

                idleStrategy.reset();
                while (!cluster.scheduleTimer(correlationId, timestamp + 100))
                {
                    idleStrategy.idle();
                }
            }

            public void onTimerEvent(final long correlationId, final long timestamp)
            {
                final String responseMsg = msg + "-scheduled";
                buffer.putStringWithoutLengthAscii(0, responseMsg);
                final ClientSession clientSession = cluster.getClientSession(clusterSessionId);

                idleStrategy.reset();
                while (clientSession.offer(buffer, 0, responseMsg.length()) < 0)
                {
                    idleStrategy.idle();
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private ClusteredServiceContainer launchServiceMessageIngressService()
    {
        final ClusteredService clusteredService = new StubClusteredService()
        {
            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                @SuppressWarnings("unused") final Header header)
            {
                if (null != session)
                {
                    idleStrategy.reset();
                    while (cluster.offer(buffer, offset, length) < 0)
                    {
                        idleStrategy.idle();
                    }
                }
                else
                {
                    cluster.forEachClientSession(
                        (clientSession) ->
                        {
                            idleStrategy.reset();
                            while (clientSession.offer(buffer, offset, length) < 0)
                            {
                                idleStrategy.idle();
                            }
                        });
                }
            }
        };

        return ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(clusteredService)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private AeronCluster connectToCluster(final EgressListener egressListener)
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .egressListener(egressListener)
                .ingressChannel("aeron:udp")
                .ingressEndpoints("0=localhost:9010,1=localhost:9011,2=localhost:9012"));
    }
}
