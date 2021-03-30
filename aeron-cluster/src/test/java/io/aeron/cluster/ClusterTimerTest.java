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

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.Tests;
import io.aeron.test.cluster.ClusterTests;
import io.aeron.test.cluster.StubClusteredService;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterTimerTest
{
    private static final long CATALOG_CAPACITY = 128 * 1024;
    private static final int INTERVAL_MS = 20;

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;
    private AeronCluster aeronCluster;

    @BeforeEach
    public void before()
    {
        launchClusteredMediaDriver(true);
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
    public void shouldRestartServiceWithTimerFromSnapshotWithFurtherLog()
    {
        final AtomicLong triggeredTimersCounter = new AtomicLong();

        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        Tests.awaitValue(triggeredTimersCounter, 2);

        final CountersReader counters = aeronCluster.context().aeron().countersReader();
        final int clusterId = clusteredMediaDriver.consensusModule().context().clusterId();
        final AtomicCounter controlToggle = ClusterControl.findControlToggle(counters, clusterId);
        assertNotNull(controlToggle);
        assertTrue(ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle));

        Tests.awaitValue(clusteredMediaDriver.consensusModule().context().snapshotCounter(), 1);
        Tests.awaitValue(triggeredTimersCounter, 4);

        forceCloseForRestart();
        triggeredTimersCounter.set(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        Tests.awaitValue(triggeredTimersCounter, 2 + 4);

        forceCloseForRestart();
        final long triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        Tests.awaitValue(triggeredTimersCounter, triggeredSinceStart + 4);

        ClusterTests.failOnClusterError();
    }

    @Test
    @Timeout(10)
    public void shouldTriggerRescheduledTimerAfterReplay()
    {
        final AtomicLong triggeredTimersCounter = new AtomicLong();

        launchReschedulingService(triggeredTimersCounter);
        connectClient();

        Tests.awaitValue(triggeredTimersCounter, 2);

        forceCloseForRestart();

        long triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);

        Tests.awaitValue(triggeredTimersCounter, triggeredSinceStart + 2);

        forceCloseForRestart();

        triggeredSinceStart = triggeredTimersCounter.getAndSet(0);

        launchClusteredMediaDriver(false);
        launchReschedulingService(triggeredTimersCounter);

        Tests.awaitValue(triggeredTimersCounter, triggeredSinceStart + 4);

        ClusterTests.failOnClusterError();
    }

    @Test
    @Timeout(10)
    void shouldRescheduleTimerWhenSchedulingWithExistingCorrelationId()
    {
        final AtomicLong timerCounter1 = new AtomicLong();
        final AtomicLong timerCounter2 = new AtomicLong();

        final ClusteredService service = new StubClusteredService()
        {
            public void onSessionOpen(final ClientSession session, final long timestamp)
            {
                schedule(1, timestamp + 1_000_000); // Too far in the future
                schedule(1, timestamp + 20);
                schedule(2, timestamp + 30);
            }

            public void onTimerEvent(final long correlationId, final long timestamp)
            {
                if (correlationId == 1)
                {
                    timerCounter1.incrementAndGet();
                }
                else
                {
                    timerCounter2.incrementAndGet();
                }
            }

            private void schedule(final long correlationId, final long deadlineMs)
            {
                idleStrategy.reset();
                while (!cluster.scheduleTimer(correlationId, deadlineMs))
                {
                    idleStrategy.idle();
                }
            }
        };

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));

        connectClient();

        Tests.awaitValue(timerCounter2, 1);
        assertEquals(1, timerCounter1.get());

        ClusterTests.failOnClusterError();
    }

    private void launchReschedulingService(final AtomicLong triggeredTimersCounter)
    {
        final ClusteredService service = new StubClusteredService()
        {
            private int timerId = 1;

            public void onTimerEvent(final long correlationId, final long timestamp)
            {
                triggeredTimersCounter.getAndIncrement();
                scheduleNext(serviceCorrelationId(timerId++), timestamp + INTERVAL_MS);
            }

            public void onStart(final Cluster cluster, final Image snapshotImage)
            {
                super.onStart(cluster, snapshotImage);
                this.cluster = cluster;

                if (null != snapshotImage)
                {
                    final FragmentHandler fragmentHandler =
                        (buffer, offset, length, header) -> timerId = buffer.getInt(offset);

                    while (true)
                    {
                        final int fragments = snapshotImage.poll(fragmentHandler, 1);
                        if (fragments == 1 || snapshotImage.isEndOfStream())
                        {
                            break;
                        }

                        idleStrategy.idle();
                    }
                }
            }

            public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
            {
                final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(SIZE_OF_INT);
                buffer.putInt(0, timerId);

                idleStrategy.reset();
                while (snapshotPublication.offer(buffer, 0, SIZE_OF_INT) < 0)
                {
                    idleStrategy.idle();
                }
            }

            public void onNewLeadershipTermEvent(
                final long leadershipTermId,
                final long logPosition,
                final long timestamp,
                final long termBaseLogPosition,
                final int leaderMemberId,
                final int logSessionId,
                final TimeUnit timeUnit,
                final int appVersion)
            {
                scheduleNext(serviceCorrelationId(timerId++), timestamp + INTERVAL_MS);
            }

            private void scheduleNext(final long correlationId, final long deadlineMs)
            {
                idleStrategy.reset();
                while (!cluster.scheduleTimer(correlationId, deadlineMs))
                {
                    idleStrategy.idle();
                }
            }
        };

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .errorHandler(ClusterTests.errorHandler(0)));
    }

    private void forceCloseForRestart()
    {
        CloseHelper.closeAll(aeronCluster, container, clusteredMediaDriver);
    }

    private void connectClient()
    {
        aeronCluster = AeronCluster.connect();
    }

    private void launchClusteredMediaDriver(final boolean initialLaunch)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .warnIfDirectoryExists(initialLaunch)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(ClusterTests.errorHandler(0))
                .dirDeleteOnStart(true),
            new Archive.Context()
                .catalogCapacity(CATALOG_CAPACITY)
                .errorHandler(ClusterTests.errorHandler(0))
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(initialLaunch),
            new ConsensusModule.Context()
                .errorHandler(ClusterTests.errorHandler(0))
                .logChannel("aeron:ipc")
                .terminationHook(ClusterTests.NOOP_TERMINATION_HOOK)
                .deleteDirOnStart(initialLaunch));
    }
}
