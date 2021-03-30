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

import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.Queue;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static io.aeron.driver.ThreadingMode.SHARED;

/**
 * Proxy for offering into the {@link Receiver} Thread's command queue.
 */
final class ReceiverProxy
{
    private final ThreadingMode threadingMode;
    private final Queue<Runnable> commandQueue;
    private final AtomicCounter failCount;

    private Receiver receiver;

    ReceiverProxy(final ThreadingMode threadingMode, final Queue<Runnable> commandQueue, final AtomicCounter failCount)
    {
        this.threadingMode = threadingMode;
        this.commandQueue = commandQueue;
        this.failCount = failCount;
    }

    void receiver(final Receiver receiver)
    {
        this.receiver = receiver;
    }

    Receiver receiver()
    {
        return receiver;
    }

    void addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        if (notConcurrent())
        {
            receiver.onAddSubscription(mediaEndpoint, streamId);
        }
        else
        {
            offer(() -> receiver.onAddSubscription(mediaEndpoint, streamId));
        }
    }

    void addSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId, final int sessionId)
    {
        if (notConcurrent())
        {
            receiver.onAddSubscription(mediaEndpoint, streamId, sessionId);
        }
        else
        {
            offer(() -> receiver.onAddSubscription(mediaEndpoint, streamId, sessionId));
        }
    }

    void removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId)
    {
        if (notConcurrent())
        {
            receiver.onRemoveSubscription(mediaEndpoint, streamId);
        }
        else
        {
            offer(() -> receiver.onRemoveSubscription(mediaEndpoint, streamId));
        }
    }

    void removeSubscription(final ReceiveChannelEndpoint mediaEndpoint, final int streamId, final int sessionId)
    {
        if (notConcurrent())
        {
            receiver.onRemoveSubscription(mediaEndpoint, streamId, sessionId);
        }
        else
        {
            offer(() -> receiver.onRemoveSubscription(mediaEndpoint, streamId, sessionId));
        }
    }

    void newPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        if (notConcurrent())
        {
            receiver.onNewPublicationImage(channelEndpoint, image);
        }
        else
        {
            offer(() -> receiver.onNewPublicationImage(channelEndpoint, image));
        }
    }

    void registerReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (notConcurrent())
        {
            receiver.onRegisterReceiveChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(() -> receiver.onRegisterReceiveChannelEndpoint(channelEndpoint));
        }
    }

    void closeReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (notConcurrent())
        {
            receiver.onCloseReceiveChannelEndpoint(channelEndpoint);
        }
        else
        {
            offer(() -> receiver.onCloseReceiveChannelEndpoint(channelEndpoint));
        }
    }

    void removeCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        if (notConcurrent())
        {
            receiver.onRemoveCoolDown(channelEndpoint, sessionId, streamId);
        }
        else
        {
            offer(() -> receiver.onRemoveCoolDown(channelEndpoint, sessionId, streamId));
        }
    }

    void addDestination(final ReceiveChannelEndpoint channelEndpoint, final ReceiveDestinationTransport transport)
    {
        if (notConcurrent())
        {
            receiver.onAddDestination(channelEndpoint, transport);
        }
        else
        {
            offer(() -> receiver.onAddDestination(channelEndpoint, transport));
        }
    }

    void removeDestination(final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel)
    {
        if (notConcurrent())
        {
            receiver.onRemoveDestination(channelEndpoint, udpChannel);
        }
        else
        {
            offer(() -> receiver.onRemoveDestination(channelEndpoint, udpChannel));
        }
    }

    void onResolutionChange(
        final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel, final InetSocketAddress newAddress)
    {
        if (notConcurrent())
        {
            receiver.onResolutionChange(channelEndpoint, udpChannel, newAddress);
        }
        else
        {
            offer(() -> receiver.onResolutionChange(channelEndpoint, udpChannel, newAddress));
        }
    }

    private boolean notConcurrent()
    {
        return threadingMode == SHARED || threadingMode == INVOKER;
    }

    private void offer(final Runnable cmd)
    {
        while (!commandQueue.offer(cmd))
        {
            if (!failCount.isClosed())
            {
                failCount.increment();
            }

            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                throw new AgentTerminationException("interrupted");
            }
        }
    }
}
