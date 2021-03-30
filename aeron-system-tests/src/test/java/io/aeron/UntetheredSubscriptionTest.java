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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UntetheredSubscriptionTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:ipc?term-length=64k",
            "aeron:udp?endpoint=localhost:24325|term-length=64k",
            "aeron-spy:aeron:udp?endpoint=localhost:24325|term-length=64k");
    }

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MESSAGE_LENGTH = 512 - DataHeaderFlyweight.HEADER_LENGTH;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .spiesSimulateConnection(true)
        .dirDeleteOnStart(true)
        .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
        .untetheredWindowLimitTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50))
        .untetheredRestingTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50))
        .threadingMode(ThreadingMode.SHARED),
        testWatcher);

    private final Aeron aeron = Aeron.connect(new Aeron.Context()
        .useConductorAgentInvoker(true));

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldBecomeUnavailableWhenNotKeepingUp(final String channel)
    {
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {};
        final AtomicBoolean unavailableCalled = new AtomicBoolean();
        final UnavailableImageHandler handler = (image) -> unavailableCalled.set(true);

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        final String untetheredChannel = channel + "|tether=false";
        final String publicationChannel = channel.startsWith("aeron-spy") ? channel.substring(10) : channel;
        boolean pollingUntethered = true;

        try (Subscription tetheredSub = aeron.addSubscription(channel, STREAM_ID);
            Subscription untetheredSub = aeron.addSubscription(untetheredChannel, STREAM_ID, null, handler);
            Publication publication = aeron.addPublication(publicationChannel, STREAM_ID))
        {
            while (!tetheredSub.isConnected() || !untetheredSub.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            while (true)
            {
                if (publication.offer(srcBuffer) < 0)
                {
                    Tests.yield();
                    aeron.conductorAgentInvoker().invoke();
                }

                if (pollingUntethered && untetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) > 0)
                {
                    pollingUntethered = false;
                }

                tetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);

                if (unavailableCalled.get())
                {
                    assertTrue(tetheredSub.isConnected());
                    assertFalse(untetheredSub.isConnected());

                    while (publication.offer(srcBuffer) < 0)
                    {
                        Tests.yield();
                        aeron.conductorAgentInvoker().invoke();
                    }

                    return;
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldRejoinAfterResting(final String channel)
    {
        final AtomicInteger unavailableImageCount = new AtomicInteger();
        final AtomicInteger availableImageCount = new AtomicInteger();
        final UnavailableImageHandler unavailableHandler = (image) -> unavailableImageCount.incrementAndGet();
        final AvailableImageHandler availableHandler = (image) -> availableImageCount.incrementAndGet();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {};

        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocate(MESSAGE_LENGTH));
        final String untetheredChannel = channel + "|tether=false";
        final String publicationChannel = channel.startsWith("aeron-spy") ? channel.substring(10) : channel;
        boolean pollingUntethered = true;

        try (Subscription tetheredSub = aeron.addSubscription(channel, STREAM_ID);
            Subscription untetheredSub = aeron.addSubscription(
                untetheredChannel, STREAM_ID, availableHandler, unavailableHandler);
            Publication publication = aeron.addPublication(publicationChannel, STREAM_ID))
        {
            while (!tetheredSub.isConnected() || !untetheredSub.isConnected())
            {
                Tests.yield();
                aeron.conductorAgentInvoker().invoke();
            }

            while (true)
            {
                if (publication.offer(srcBuffer) < 0)
                {
                    Tests.yield();
                    aeron.conductorAgentInvoker().invoke();
                }

                if (pollingUntethered && untetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) > 0)
                {
                    pollingUntethered = false;
                }

                tetheredSub.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);

                if (unavailableImageCount.get() == 1)
                {
                    while (availableImageCount.get() < 2)
                    {
                        Tests.yield();
                        aeron.conductorAgentInvoker().invoke();
                    }

                    return;
                }
            }
        }
    }
}
