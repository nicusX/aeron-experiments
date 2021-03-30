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
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaggedMulticastFlowControlTest
{
    private static final int DEFAULT_GROUP_SIZE = 0;
    private static final long DEFAULT_GROUP_TAG = Configuration.flowControlGroupTag();
    private static final long DEFAULT_TIMEOUT = Configuration.flowControlReceiverTimeoutNs();
    private static final int WINDOW_LENGTH = 16 * 1024;

    private final TaggedMulticastFlowControl flowControl = new TaggedMulticastFlowControl();

    private static Stream<Arguments> validUris()
    {
        return Stream.of(
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged",
                DEFAULT_GROUP_TAG, DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,t:100ms",
                DEFAULT_GROUP_TAG, DEFAULT_GROUP_SIZE, 100_000_000),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123",
                123, DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:3000000000",
                3_000_000_000L, DEFAULT_GROUP_SIZE, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123,t:100ms",
                123, DEFAULT_GROUP_SIZE, 100_000_000),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/10",
                100, 10, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:/10",
                DEFAULT_GROUP_TAG, 10, DEFAULT_TIMEOUT),
            Arguments.of(
                "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/10,t:100ms",
                100, 10, 100_000_000));
    }

    @ParameterizedTest
    @MethodSource("validUris")
    void shouldParseValidFlowControlConfiguration(
        final String uri, final long groupTag, final int groupSize, final long timeout)
    {
        flowControl.initialize(new MediaDriver.Context(), UdpChannel.parse(uri), 0, 0);

        assertEquals(groupTag, flowControl.groupTag());
        assertEquals(groupSize, flowControl.groupMinSize());
        assertEquals(timeout, flowControl.receiverTimeoutNs());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:/",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,t:",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100,t:",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,t:100ms,g:100/",
    })
    void shouldFailWithInvalidUris(final String uri)
    {
        assertThrows(
            Exception.class,
            () -> flowControl.initialize(new MediaDriver.Context(), UdpChannel.parse(uri), 0, 0));
    }

    @Test
    void shouldClampToSenderLimitUntilMinimumGroupSizeIsMet()
    {
        final UdpChannel channelGroupSizeThree = UdpChannel.parse(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/3");

        flowControl.initialize(new MediaDriver.Context(), channelGroupSizeThree, 0, 0);
        final long groupTag = 123L;

        final long senderLimit = 5000L;
        final int termOffset = 10_000;

        assertEquals(senderLimit, onStatusMessage(flowControl, 0, termOffset, senderLimit, null));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(senderLimit, onStatusMessage(flowControl, 1, termOffset, senderLimit, groupTag));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(senderLimit, onStatusMessage(flowControl, 2, termOffset, senderLimit, groupTag));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(senderLimit, onStatusMessage(flowControl, 3, termOffset, senderLimit, null));
        assertEquals(senderLimit, onIdle(flowControl, senderLimit));
        assertEquals(termOffset + WINDOW_LENGTH, onStatusMessage(flowControl, 4, termOffset, senderLimit, groupTag));
        assertEquals(termOffset + WINDOW_LENGTH, onIdle(flowControl, senderLimit));
    }

    @Test
    void shouldReturnLastWindowWhenUntilReceiversAreInGroupWithNoMinSize()
    {
        final UdpChannel channelGroupSizeThree = UdpChannel.parse(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123");

        flowControl.initialize(new MediaDriver.Context(), channelGroupSizeThree, 0, 0);
        final long groupTag = 123L;

        final long senderLimit = 5000L;
        final int termOffset0 = 10_000;
        final int termOffset1 = 9_999;

        assertEquals(termOffset0 + WINDOW_LENGTH, onStatusMessage(flowControl, 0, termOffset0, senderLimit, null));
        assertEquals(
            termOffset1 + WINDOW_LENGTH, onStatusMessage(flowControl, 1, termOffset1, senderLimit, groupTag));
        assertEquals(termOffset1 + WINDOW_LENGTH, onStatusMessage(flowControl, 0, termOffset0, senderLimit, null));
    }

    private long onStatusMessage(
        final TaggedMulticastFlowControl flowControl,
        final long receiverId,
        final int termOffset,
        final long senderLimit,
        final Long groupTag)
    {
        final StatusMessageFlyweight statusMessageFlyweight = new StatusMessageFlyweight();
        statusMessageFlyweight.wrap(new byte[1024]);

        statusMessageFlyweight.receiverId(receiverId);
        statusMessageFlyweight.consumptionTermId(0);
        statusMessageFlyweight.consumptionTermOffset(termOffset);
        statusMessageFlyweight.groupTag(groupTag);
        statusMessageFlyweight.receiverWindowLength(WINDOW_LENGTH);

        return flowControl.onStatusMessage(statusMessageFlyweight, null, senderLimit, 0, 0, 0);
    }

    private long onIdle(final TaggedMulticastFlowControl flowControl, final long senderLimit)
    {
        return flowControl.onIdle(0, senderLimit, 0, false);
    }
}
