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

import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.StatusMessageFlyweight;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MinMulticastFlowControlTest
{
    private static final int DEFAULT_GROUP_SIZE = 3;
    private static final long DEFAULT_TIMEOUT = Configuration.flowControlReceiverTimeoutNs();
    private static final int WINDOW_LENGTH = 16 * 1024;

    private final MinMulticastFlowControl flowControl = new MinMulticastFlowControl();

    private static Stream<Arguments> validUris()
    {
        return Stream.of(
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min",
                DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,t:100ms",
                DEFAULT_GROUP_SIZE, 100_000_000),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:123",
                DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:3000000000",
                DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:123,t:100ms",
                DEFAULT_GROUP_SIZE, 100_000_000),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:100/10",
                10, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/10",
                10, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:100/10,t:100ms",
                10, 100_000_000));
    }

    @ParameterizedTest
    @MethodSource("validUris")
    void shouldParseValidFlowControlConfiguration(final String uri, final int groupSize, final long timeout)
    {
        flowControl.initialize(
            new MediaDriver.Context().flowControlGroupMinSize(DEFAULT_GROUP_SIZE),
            UdpChannel.parse(uri), 0, 0);

        assertEquals(groupSize, flowControl.groupMinSize());
        assertEquals(timeout, flowControl.receiverTimeoutNs());
    }

    @Test
    void shouldNotBeConnectedUntilGroupMinSizeReached()
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");

        flowControl.initialize(new MediaDriver.Context(), udpChannel, 0, 0);

        onStatusMessage(flowControl, 1, 0, 5000);
        assertFalse(flowControl.hasRequiredReceivers());
        onStatusMessage(flowControl, 2, 0, 5000);
        assertFalse(flowControl.hasRequiredReceivers());
        onStatusMessage(flowControl, 2, 0, 5000);
        assertFalse(flowControl.hasRequiredReceivers());
        onStatusMessage(flowControl, 3, 0, 5000);
        assertTrue(flowControl.hasRequiredReceivers());
    }

    @Test
    void shouldReportSenderLimitUntilGroupMinSizeIsReached()
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/3");

        flowControl.initialize(new MediaDriver.Context(), udpChannel, 0, 0);

        final int senderLimit = 5000;
        final int termOffset = 6000;
        assertEquals(senderLimit, onStatusMessage(flowControl, 1, termOffset, senderLimit));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(senderLimit, onStatusMessage(flowControl, 2, termOffset, senderLimit));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(senderLimit, onStatusMessage(flowControl, 2, termOffset, senderLimit));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(termOffset + WINDOW_LENGTH, onStatusMessage(flowControl, 3, termOffset, senderLimit));
        assertEquals(termOffset + WINDOW_LENGTH, onIdle(flowControl, senderLimit));
    }

    private long onStatusMessage(
        final MinMulticastFlowControl flowControl, final long receiverId, final int termOffset, final long senderLimit)
    {
        final StatusMessageFlyweight statusMessageFlyweight = new StatusMessageFlyweight();
        statusMessageFlyweight.wrap(new byte[1024]);

        statusMessageFlyweight.receiverId(receiverId);
        statusMessageFlyweight.consumptionTermId(0);
        statusMessageFlyweight.consumptionTermOffset(termOffset);
        statusMessageFlyweight.receiverWindowLength(WINDOW_LENGTH);

        return flowControl.onStatusMessage(statusMessageFlyweight, null, senderLimit, 0, 0, 0);
    }

    private long onIdle(final MinMulticastFlowControl flowControl, final long senderLimit)
    {
        return flowControl.onIdle(0, senderLimit, 0, false);
    }
}