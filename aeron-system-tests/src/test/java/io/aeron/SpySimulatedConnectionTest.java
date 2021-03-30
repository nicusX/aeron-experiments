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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

public class SpySimulatedConnectionTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=localhost:24325",
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost");
    }

    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;

    private final MediaDriver.Context driverContext = new MediaDriver.Context();

    private Aeron client;
    private TestMediaDriver driver;
    private Publication publication;
    private Subscription subscription;
    private Subscription spy;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);

    private final MutableInteger fragmentCountSpy = new MutableInteger();
    private final FragmentHandler fragmentHandlerSpy = (buffer1, offset, length, header) -> fragmentCountSpy.value++;

    private final MutableInteger fragmentCountSub = new MutableInteger();
    private final FragmentHandler fragmentHandlerSub = (buffer1, offset, length, header) -> fragmentCountSub.value++;

    @RegisterExtension
    public final MediaDriverTestWatcher watcher = new MediaDriverTestWatcher();

    private void launch()
    {
        driverContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverContext, watcher);
        client = Aeron.connect();
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(client, driver);
        driverContext.deleteDirectory();
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldNotSimulateConnectionWhenNotConfigured(final String channel)
    {
        launch();

        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        publication = client.addPublication(channel, STREAM_ID);

        Tests.awaitConnected(spy);

        assertFalse(publication.isConnected());
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldSimulateConnectionWhenOnChannel(final String channel)
    {
        launch();

        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        publication = client.addPublication(channel + "|ssc=true", STREAM_ID);

        Tests.awaitConnected(spy);
        Tests.awaitConnected(publication);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldSimulateConnectionWithNoNetworkSubscriptions(final String channel)
    {
        final int messagesToSend = NUM_MESSAGES_PER_TERM * 3;

        driverContext
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .spiesSimulateConnection(true);

        launch();

        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        publication = client.addPublication(channel, STREAM_ID);

        while (!spy.isConnected() || !publication.isConnected())
        {
            Tests.yield();
        }

        for (int i = 0; i < messagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
            Tests.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    final int fragments = spy.poll(fragmentHandlerSpy, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        assertEquals(messagesToSend, fragmentCountSpy.value);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldSimulateConnectionWithSlowNetworkSubscription(final String channel)
    {
        final int messagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int messagesLeftToSend = messagesToSend;
        int fragmentsFromSpy = 0;
        int fragmentsFromSubscription = 0;

        driverContext
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .spiesSimulateConnection(true);

        launch();

        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        subscription = client.addSubscription(channel, STREAM_ID);
        publication = client.addPublication(channel, STREAM_ID);

        waitUntilFullConnectivity();

        for (int i = 0; fragmentsFromSpy < messagesToSend || fragmentsFromSubscription < messagesToSend; i++)
        {
            if (messagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    messagesLeftToSend--;
                }
            }

            Tests.yield();

            fragmentsFromSpy += spy.poll(fragmentHandlerSpy, 10);

            // subscription receives slowly
            if ((i % 2) == 0)
            {
                fragmentsFromSubscription += subscription.poll(fragmentHandlerSub, 1);
            }
        }

        assertEquals(messagesToSend, fragmentCountSpy.value);
        assertEquals(messagesToSend, fragmentCountSub.value);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @Timeout(10)
    public void shouldSimulateConnectionWithLeavingNetworkSubscription(final String channel)
    {
        final int messagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int messagesLeftToSend = messagesToSend;
        int fragmentsReadFromSpy = 0;
        int fragmentsReadFromSubscription = 0;
        boolean isSubscriptionClosed = false;

        driverContext
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .spiesSimulateConnection(true);

        launch();

        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        subscription = client.addSubscription(channel, STREAM_ID);
        publication = client.addPublication(channel, STREAM_ID);

        waitUntilFullConnectivity();

        while (fragmentsReadFromSpy < messagesToSend)
        {
            if (messagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    messagesLeftToSend--;
                }
                else
                {
                    Tests.yield();
                }
            }

            fragmentsReadFromSpy += spy.poll(fragmentHandlerSpy, 10);

            // subscription receives up to 1/8 of the messages, then stops
            if (fragmentsReadFromSubscription < (messagesToSend / 8))
            {
                fragmentsReadFromSubscription += subscription.poll(fragmentHandlerSub, 10);
            }
            else if (!isSubscriptionClosed)
            {
                subscription.close();
                isSubscriptionClosed = true;
            }
        }

        assertEquals(messagesToSend, fragmentCountSpy.value);
    }

    private void waitUntilFullConnectivity()
    {
        while (!spy.isConnected() || !subscription.isConnected() || !publication.isConnected())
        {
            Tests.yield();
        }

        // send initial message to ensure connectivity
        while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
        {
            Tests.yield();
        }

        final FragmentHandler mockFragmentHandler = mock(FragmentHandler.class);

        while (spy.poll(mockFragmentHandler, 1) == 0)
        {
            Tests.yield();
        }

        while (subscription.poll(mockFragmentHandler, 1) == 0)
        {
            Tests.yield();
        }
    }

    private static String spyForChannel(final String channel)
    {
        return CommonContext.SPY_PREFIX + channel;
    }
}
