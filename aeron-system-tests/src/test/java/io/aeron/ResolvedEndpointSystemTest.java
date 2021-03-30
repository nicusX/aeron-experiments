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
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;

public class ResolvedEndpointSystemTest
{
    private static final int STREAM_ID = 2002;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);

    private TestMediaDriver driver;
    private Aeron client;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @BeforeEach
    void before()
    {
        buffer.putInt(0, 1);

        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(context, testWatcher);
        client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(client, driver);
        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    @Timeout(5)
    void shouldSubscribeWithSystemAssignedPort()
    {
        final String uri = "aeron:udp?endpoint=localhost:0";

        try (Subscription sub = client.addSubscription(uri, STREAM_ID))
        {
            String resolvedUri;
            while (null == (resolvedUri = sub.tryResolveChannelEndpointPort()))
            {
                Tests.yieldingWait("No bind address/port for sub");
            }

            assertThat(resolvedUri, startsWith("aeron:udp?endpoint=localhost:"));

            try (Publication pub = client.addPublication(resolvedUri, STREAM_ID))
            {
                while (pub.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub");
                }

                while (sub.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub");
                }
            }
        }
    }

    @Test
    @Timeout(5)
    void shouldSubscribeToSystemAssignedPorts()
    {
        final String systemAssignedPortUri1 = "aeron:udp?endpoint=127.0.0.1:0|tags=1002";
        final String systemAssignedPortUri2 = "aeron:udp?endpoint=127.0.0.1:0|tags=1003";
        final String tagged1 = "aeron:udp?tags=1002";

        try (Subscription sub1 = client.addSubscription(systemAssignedPortUri1, STREAM_ID);
            Subscription sub2 = client.addSubscription(systemAssignedPortUri2, STREAM_ID);
            Subscription sub3 = client.addSubscription(tagged1, STREAM_ID + 1))
        {
            List<String> bindAddressAndPort1;
            while ((bindAddressAndPort1 = sub1.localSocketAddresses()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub1");
            }

            List<String> bindAddressAndPort2;
            while ((bindAddressAndPort2 = sub2.localSocketAddresses()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub2");
            }

            assertNotEquals(bindAddressAndPort1, bindAddressAndPort2);

            List<String> bindAddressAndPort3;
            while ((bindAddressAndPort3 = sub3.localSocketAddresses()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub3");
            }

            assertEquals(bindAddressAndPort3, bindAddressAndPort1);

            final String pubUri = "aeron:udp?endpoint=" + bindAddressAndPort1.get(0);

            try (Publication pub = client.addPublication(pubUri, STREAM_ID))
            {
                while (pub.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub");
                }

                while (sub1.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub1");
                }
            }
        }
    }

    @Test
    @Timeout(5)
    void shouldSubscribeToSystemAssignedPortsUsingIPv6()
    {
        assumeFalse(Boolean.getBoolean("java.net.preferIPv4Stack"));

        final String systemAssignedPortUri = "aeron:udp?endpoint=[::1]:0|tags=1001";
        final String tagged2 = "aeron:udp?tags=1001";

        try (Subscription sub1 = client.addSubscription(systemAssignedPortUri, STREAM_ID);
            Subscription sub2 = client.addSubscription(tagged2, STREAM_ID + 1))
        {
            List<String> bindAddressAndPort1;
            while ((bindAddressAndPort1 = sub1.localSocketAddresses()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub1");
            }

            List<String> bindAddressAndPort2;
            while ((bindAddressAndPort2 = sub2.localSocketAddresses()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub2");
            }

            assertEquals(bindAddressAndPort2, bindAddressAndPort1);

            final String pubUri = "aeron:udp?endpoint=" + bindAddressAndPort1.get(0);

            try (Publication pub = client.addPublication(pubUri, STREAM_ID))
            {
                while (pub.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub");
                }

                while (sub1.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub1");
                }
            }
        }
    }

    @Test
    @Timeout(5)
    void shouldBindMultipleSystemAssignedEndpointsForMultiDestinationSubscription()
    {
        final String systemAssignedPortUri1 = "aeron:udp?endpoint=127.0.0.1:0";
        final String systemAssignedPortUri2 = "aeron:udp?endpoint=127.0.0.1:0";

        try (Subscription mdsSub = client.addSubscription("aeron:udp?control-mode=manual", STREAM_ID))
        {
            mdsSub.addDestination(systemAssignedPortUri1);
            mdsSub.addDestination(systemAssignedPortUri2);

            List<String> bindAddressAndPorts;
            while (2 > (bindAddressAndPorts = mdsSub.localSocketAddresses()).size())
            {
                Tests.yieldingWait("Unable to get bind address/ports for mds subscription");
            }

            final String pub1Uri = "aeron:udp?endpoint=" + bindAddressAndPorts.get(0);
            final String pub2Uri = "aeron:udp?endpoint=" + bindAddressAndPorts.get(1);

            try (Publication pub1 = client.addPublication(pub1Uri, STREAM_ID);
                Publication pub2 = client.addPublication(pub2Uri, STREAM_ID))
            {
                while (pub1.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub1");
                }

                while (pub2.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub2");
                }

                long totalReceived = 0;
                while ((totalReceived += mdsSub.poll(fragmentHandler, 10)) < 2)
                {
                    Tests.yieldingWait("Failed to receive from both publications");
                }
            }
        }
    }

    @Test
    @Timeout(5)
    void shouldAllowSystemAssignedPortOnDynamicMultiDestinationPublication()
    {
        final String mdcUri = "aeron:udp?control=localhost:0";

        try (Publication pub = client.addPublication(mdcUri, STREAM_ID))
        {
            List<String> bindAddressAndPort1;
            while ((bindAddressAndPort1 = pub.localSocketAddresses()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for pub");
            }

            final String mdcSubUri = new ChannelUriStringBuilder()
                .media("udp")
                .controlEndpoint(bindAddressAndPort1.get(0))
                .group(Boolean.TRUE)
                .build();

            try (Subscription sub = client.addSubscription(mdcSubUri, STREAM_ID))
            {
                while (pub.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub");
                }

                while (sub.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub");
                }
            }
        }
    }
}
