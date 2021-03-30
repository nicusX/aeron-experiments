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
package io.aeron.driver;

import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.aeron.CommonContext;
import io.aeron.DriverProxy;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class IpcPublicationTest
{
    private static final long CLIENT_ID = 7L;
    private static final int STREAM_ID = 1010;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int BUFFER_LENGTH = 16 * 1024;

    private Position publisherLimit;
    private IpcPublication ipcPublication;

    private DriverProxy driverProxy;
    private DriverConductor driverConductor;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp()
    {
        final RingBuffer toDriverCommands = new ManyToOneRingBuffer(new UnsafeBuffer(
            ByteBuffer.allocate(Configuration.CONDUCTOR_BUFFER_LENGTH_DEFAULT)));

        final UnsafeBuffer counterBuffer = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_LENGTH));
        final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(
            ByteBuffer.allocate(Configuration.countersMetadataBufferLength(BUFFER_LENGTH)));
        final CountersManager countersManager = new CountersManager(
            metaDataBuffer, counterBuffer, StandardCharsets.US_ASCII);
        final SystemCounters systemCounters = new SystemCounters(countersManager);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .tempBuffer(new UnsafeBuffer(new byte[METADATA_LENGTH]))
            .ipcTermBufferLength(TERM_BUFFER_LENGTH)
            .toDriverCommands(toDriverCommands)
            .logFactory(new TestLogFactory())
            .clientProxy(mock(ClientProxy.class))
            .driverCommandQueue(mock(ManyToOneConcurrentArrayQueue.class))
            .epochClock(SystemEpochClock.INSTANCE)
            .cachedEpochClock(new CachedEpochClock())
            .cachedNanoClock(new CachedNanoClock())
            .countersManager(countersManager)
            .systemCounters(systemCounters)
            .nameResolver(DefaultNameResolver.INSTANCE)
            .nanoClock(new CachedNanoClock());

        ctx.countersValuesBuffer(counterBuffer);

        driverProxy = new DriverProxy(toDriverCommands, CLIENT_ID);
        driverConductor = new DriverConductor(ctx);
        driverConductor.onStart();

        driverProxy.addPublication(CommonContext.IPC_CHANNEL, STREAM_ID);
        driverConductor.doWork();

        ipcPublication = driverConductor.getSharedIpcPublication(STREAM_ID);
        publisherLimit = new UnsafeBufferPosition(counterBuffer, ipcPublication.publisherLimitId());
    }

    @Test
    public void shouldStartWithPublisherLimitSetToZero()
    {
        assertThat(publisherLimit.get(), is(0L));
    }

    @Test
    public void shouldKeepPublisherLimitZeroOnNoSubscriptionUpdate()
    {
        ipcPublication.updatePublisherLimit();
        assertThat(publisherLimit.get(), is(0L));
    }

    @Test
    public void shouldHaveJoiningPositionZeroWhenNoSubscriptions()
    {
        assertThat(ipcPublication.joinPosition(), is(0L));
    }

    @Test
    public void shouldIncrementPublisherLimitOnSubscription()
    {
        driverProxy.addSubscription(CommonContext.IPC_CHANNEL, STREAM_ID);
        driverConductor.doWork();

        assertThat(publisherLimit.get(), is(greaterThan(0L)));
    }
}
