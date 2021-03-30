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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.CommonContext.SPY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MinPositionSubscriptionTest
{
    private static final int STREAM_ID = 1001;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private final TestMediaDriver driver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .dirDeleteOnStart(true)
        .spiesSimulateConnection(true)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED),
        testWatcher);

    private final Aeron aeron = Aeron.connect();

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeron, driver);
        driver.context().deleteDirectory();
    }

    @Timeout(10)
    @Test
    public void shouldJoinAtSamePositionIpc()
    {
        final String channel = "aeron:ipc";
        shouldJoinAtSamePosition(channel, channel);
    }

    @Timeout(10)
    @Test
    public void shouldJoinAtSamePositionUdp()
    {
        final String channel = "aeron:udp?endpoint=localhost:24325";
        shouldJoinAtSamePosition(channel, channel);
    }

    @Timeout(10)
    @Test
    public void shouldJoinAtSamePositionUdpSpy()
    {
        final String channel = "aeron:udp?endpoint=localhost:24325";
        shouldJoinAtSamePosition(channel, SPY_PREFIX + channel);
    }

    private void shouldJoinAtSamePosition(final String publicationChannel, final String subscriptionChannel)
    {
        try (Subscription subscriptionOne = aeron.addSubscription(subscriptionChannel, STREAM_ID);
            Publication publication = aeron.addPublication(publicationChannel, STREAM_ID))
        {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[128]);

            while (publication.offer(srcBuffer) < 0L)
            {
                Tests.yield();
            }

            final int sessionId = publication.sessionId();
            publication.close();

            Image imageOne;
            while (null == (imageOne = subscriptionOne.imageBySessionId(sessionId)))
            {
                Tests.yield();
            }

            assertEquals(0L, imageOne.joinPosition());

            try (Subscription subscriptionTwo = aeron.addSubscription(subscriptionChannel, STREAM_ID))
            {
                Image imageTwo;
                while (null == (imageTwo = subscriptionTwo.imageBySessionId(sessionId)))
                {
                    Tests.yield();
                }

                assertEquals(imageOne.joinPosition(), imageTwo.joinPosition());
            }
        }
    }
}
