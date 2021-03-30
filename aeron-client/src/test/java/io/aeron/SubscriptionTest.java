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

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.status.ChannelEndpointStatus.*;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SubscriptionTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40124";
    private static final int STREAM_ID_1 = 1002;
    private static final int INITIAL_TERM_ID = 7;
    private static final long SUBSCRIPTION_CORRELATION_ID = 100;
    private static final long REGISTRATION_ID = 10;
    private static final int READ_BUFFER_CAPACITY = 1024;
    private static final int FRAGMENT_COUNT_LIMIT = Integer.MAX_VALUE;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;

    private final UnsafeBuffer atomicReadBuffer = new UnsafeBuffer(ByteBuffer.allocate(READ_BUFFER_CAPACITY));
    private final ClientConductor conductor = mock(ClientConductor.class);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final Image imageOneMock = mock(Image.class);
    private final Image imageTwoMock = mock(Image.class);
    private final Header header = new Header(
        INITIAL_TERM_ID, LogBufferDescriptor.positionBitsToShift(LogBufferDescriptor.TERM_MIN_LENGTH));
    private final AvailableImageHandler availableImageHandlerMock = mock(AvailableImageHandler.class);
    private final UnavailableImageHandler unavailableImageHandlerMock = mock(UnavailableImageHandler.class);

    private final UnsafeBuffer valuesBuffer = new UnsafeBuffer(ByteBuffer.allocate(16 * 1024));
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(ByteBuffer.allocate(64 * 1024));
    private final UnsafeBuffer tempBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));
    private final CountersManager countersManager = new CountersManager(metaDataBuffer, valuesBuffer, US_ASCII);

    private Subscription subscription;

    @BeforeEach
    public void setUp()
    {
        when(imageOneMock.correlationId()).thenReturn(1L);
        when(imageTwoMock.correlationId()).thenReturn(2L);

        when(conductor.countersReader()).thenReturn(countersManager);

        subscription = new Subscription(
            conductor,
            CHANNEL,
            STREAM_ID_1,
            SUBSCRIPTION_CORRELATION_ID,
            availableImageHandlerMock,
            unavailableImageHandlerMock);

        doAnswer(
            (invocation) ->
            {
                subscription.internalClose(NULL_VALUE);
                return null;
            }).when(conductor).releaseSubscription(subscription);
    }

    @Test
    public void shouldEnsureTheSubscriptionIsOpenWhenPolling()
    {
        subscription.close();
        assertTrue(subscription.isClosed());

        verify(conductor).releaseSubscription(subscription);
    }

    @Test
    public void shouldReadNothingWhenNoImages()
    {
        assertEquals(0, subscription.poll(fragmentHandler, 1));
    }

    @Test
    public void shouldReadNothingWhenThereIsNoData()
    {
        subscription.addImage(imageOneMock);

        assertEquals(0, subscription.poll(fragmentHandler, 1));
    }

    @Test
    public void shouldReadData()
    {
        subscription.addImage(imageOneMock);

        when(imageOneMock.poll(any(FragmentHandler.class), anyInt())).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertEquals(1, subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT));
        verify(fragmentHandler).onFragment(
            eq(atomicReadBuffer),
            eq(HEADER_LENGTH),
            eq(READ_BUFFER_CAPACITY - HEADER_LENGTH),
            any(Header.class));
    }

    @Test
    public void shouldReadDataFromMultipleSources()
    {
        subscription.addImage(imageOneMock);
        subscription.addImage(imageTwoMock);

        when(imageOneMock.poll(any(FragmentHandler.class), anyInt())).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        when(imageTwoMock.poll(any(FragmentHandler.class), anyInt())).then(
            (invocation) ->
            {
                final FragmentHandler handler = (FragmentHandler)invocation.getArguments()[0];
                handler.onFragment(atomicReadBuffer, HEADER_LENGTH, READ_BUFFER_CAPACITY - HEADER_LENGTH, header);

                return 1;
            });

        assertEquals(2, subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT));
    }

    @ValueSource(longs = { INITIALIZING, ERRORED, CLOSING })
    @ParameterizedTest
    void tryResolveChannelEndpointPortReturnsNullIfChannelStatusIsNotActive(final long channelStatus)
    {
        final int channelStatusId = 555;
        subscription.channelStatusId(channelStatusId);
        when(conductor.channelStatus(channelStatusId)).thenReturn(channelStatus);

        assertNull(subscription.tryResolveChannelEndpointPort());
    }

    @Test
    void tryResolveChannelEndpointPortReturnsNullIfSubscriptionIsClosed()
    {
        subscription.close();
        assertTrue(subscription.isClosed());

        assertNull(subscription.tryResolveChannelEndpointPort());
    }

    @Test
    void tryResolveChannelEndpointPortReturnsOriginalChannelIfNoAddressesFound()
    {
        final int channelStatusId = 123;
        subscription.channelStatusId(channelStatusId);
        when(conductor.channelStatus(channelStatusId)).thenReturn(ACTIVE);

        assertSame(CHANNEL, subscription.tryResolveChannelEndpointPort());
    }

    @Test
    void tryResolveChannelEndpointPortReturnsOriginalChannelIfMoreThanOneAddressFound()
    {
        final int channelStatusId = 123;
        subscription.channelStatusId(channelStatusId);
        when(conductor.channelStatus(channelStatusId)).thenReturn(ACTIVE);

        allocateAddressCounter("localhost:5555", channelStatusId, ACTIVE);
        allocateAddressCounter("localhost:7777", channelStatusId, ACTIVE);

        assertSame(CHANNEL, subscription.tryResolveChannelEndpointPort());
    }

    @Test
    void tryResolveChannelEndpointPortReturnsOriginalChannelIfNonZeroPortWasSpecified()
    {
        final int channelStatusId = 444;
        final String channel = "aeron:udp?endpoint=localhost:40124|interface=192.168.5.0/24|reliable=false";
        subscription = new Subscription(
            conductor,
            channel,
            STREAM_ID_1,
            SUBSCRIPTION_CORRELATION_ID,
            availableImageHandlerMock,
            unavailableImageHandlerMock);
        subscription.channelStatusId(channelStatusId);
        when(conductor.channelStatus(channelStatusId)).thenReturn(ACTIVE);

        allocateAddressCounter("127.0.0.1:19091", channelStatusId, ACTIVE);
        allocateAddressCounter("localhost:21212", channelStatusId, ERRORED);

        assertSame(channel, subscription.tryResolveChannelEndpointPort());
    }

    @Test
    void tryResolveChannelEndpointPortReturnsChannelWithResolvedPort()
    {
        final int channelStatusId = 444;
        final String channel = "aeron:udp?endpoint=localhost:0|interface=192.168.5.0/24|reliable=false";
        subscription = new Subscription(
            conductor,
            channel,
            STREAM_ID_1,
            SUBSCRIPTION_CORRELATION_ID,
            availableImageHandlerMock,
            unavailableImageHandlerMock);
        subscription.channelStatusId(channelStatusId);
        when(conductor.channelStatus(channelStatusId)).thenReturn(ACTIVE);

        allocateAddressCounter("127.0.0.1:19091", channelStatusId, ACTIVE);
        allocateAddressCounter("localhost:21212", channelStatusId, ERRORED);

        final String channelWithResolvedEndpoint = subscription.tryResolveChannelEndpointPort();

        assertEquals(
            ChannelUri.parse("aeron:udp?endpoint=localhost:19091|interface=192.168.5.0/24|reliable=false"),
            ChannelUri.parse(channelWithResolvedEndpoint));
    }

    private void allocateAddressCounter(final String address, final int channelStatusId, final long status)
    {
        final AtomicCounter counter = LocalSocketAddressStatus.allocate(
            tempBuffer,
            countersManager,
            REGISTRATION_ID,
            channelStatusId,
            "test",
            LocalSocketAddressStatus.LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID);

        LocalSocketAddressStatus.updateBindAddress(counter, address, (UnsafeBuffer)countersManager.metaDataBuffer());
        counter.setOrdered(status);
    }
}
