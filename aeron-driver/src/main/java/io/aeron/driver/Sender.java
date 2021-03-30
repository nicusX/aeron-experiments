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

import io.aeron.ChannelUri;
import io.aeron.driver.media.ControlTransportPoller;
import io.aeron.driver.media.SendChannelEndpoint;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;

import static io.aeron.driver.status.SystemCounterDescriptor.BYTES_SENT;
import static io.aeron.driver.status.SystemCounterDescriptor.RESOLUTION_CHANGES;

class SenderLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

class SenderHotFields extends SenderLhsPadding
{
    long controlPollDeadlineNs;
    long reResolutionDeadlineNs;
    int dutyCycleCounter;
    int roundRobinIndex = 0;
}

class SenderRhsPadding extends SenderHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

/**
 * Agent that iterates over {@link NetworkPublication}s for sending them to {@link Receiver}s on behalf of registered
 * subscribers.
 */
public final class Sender extends SenderRhsPadding implements Agent
{
    private NetworkPublication[] networkPublications = new NetworkPublication[0];

    private final long statusMessageReadTimeoutNs;
    private final long reResolutionCheckIntervalNs;
    private final int dutyCycleRatio;
    private final ControlTransportPoller controlTransportPoller;
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue;
    private final AtomicCounter totalBytesSent;
    private final AtomicCounter resolutionChanges;
    private final NanoClock nanoClock;
    private final CachedNanoClock cachedNanoClock;
    private final DriverConductorProxy conductorProxy;

    Sender(final MediaDriver.Context ctx)
    {
        this.controlTransportPoller = ctx.controlTransportPoller();
        this.commandQueue = ctx.senderCommandQueue();
        this.totalBytesSent = ctx.systemCounters().get(BYTES_SENT);
        this.resolutionChanges = ctx.systemCounters().get(RESOLUTION_CHANGES);
        this.nanoClock = ctx.nanoClock();
        this.cachedNanoClock = ctx.senderCachedNanoClock();
        this.statusMessageReadTimeoutNs = ctx.statusMessageTimeoutNs() >> 1;
        this.reResolutionCheckIntervalNs = ctx.reResolutionCheckIntervalNs();
        this.dutyCycleRatio = ctx.sendToStatusMessagePollRatio();
        this.conductorProxy = ctx.driverConductorProxy();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        controlTransportPoller.close();
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);

        final int workCount = commandQueue.drain(Runnable::run, Configuration.COMMAND_DRAIN_LIMIT);
        final int bytesSent = doSend(nowNs);

        int bytesReceived = 0;
        if (0 == bytesSent || ++dutyCycleCounter >= dutyCycleRatio || (controlPollDeadlineNs - nowNs < 0))
        {
            bytesReceived = controlTransportPoller.pollTransports();

            dutyCycleCounter = 0;
            controlPollDeadlineNs = nowNs + statusMessageReadTimeoutNs;
        }

        if (reResolutionCheckIntervalNs > 0 && (reResolutionDeadlineNs - nowNs) < 0)
        {
            controlTransportPoller.checkForReResolutions(nowNs, conductorProxy);
            reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
        }

        return workCount + bytesSent + bytesReceived;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "sender";
    }

    void onRegisterSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        channelEndpoint.openChannel(conductorProxy);
        channelEndpoint.registerForRead(controlTransportPoller);
        channelEndpoint.indicateActive();
    }

    void onCloseSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        channelEndpoint.close();
    }

    void onNewNetworkPublication(final NetworkPublication publication)
    {
        networkPublications = ArrayUtil.add(networkPublications, publication);
        publication.channelEndpoint().registerForSend(publication);
    }

    void onRemoveNetworkPublication(final NetworkPublication publication)
    {
        networkPublications = ArrayUtil.remove(networkPublications, publication);
        publication.channelEndpoint().unregisterForSend(publication);
        publication.senderRelease();
    }

    void onAddDestination(
        final SendChannelEndpoint channelEndpoint, final ChannelUri channelUri, final InetSocketAddress address)
    {
        channelEndpoint.addDestination(channelUri, address);
    }

    void onRemoveDestination(
        final SendChannelEndpoint channelEndpoint, final ChannelUri channelUri, final InetSocketAddress address)
    {
        channelEndpoint.removeDestination(channelUri, address);
    }

    void onResolutionChange(
        final SendChannelEndpoint channelEndpoint, final String endpoint, final InetSocketAddress newAddress)
    {
        channelEndpoint.resolutionChange(endpoint, newAddress);
        resolutionChanges.getAndAddOrdered(1);
    }

    private int doSend(final long nowNs)
    {
        int bytesSent = 0;
        final NetworkPublication[] publications = this.networkPublications;
        final int length = publications.length;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        for (int i = startingIndex; i < length; i++)
        {
            bytesSent += publications[i].send(nowNs);
        }

        for (int i = 0; i < startingIndex; i++)
        {
            bytesSent += publications[i].send(nowNs);
        }

        totalBytesSent.getAndAddOrdered(bytesSent);

        return bytesSent;
    }
}
