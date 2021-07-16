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

import io.aeron.AeronCounters;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.media.UdpNameResolutionTransport;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.DriverNameResolverCache.byteSubsetEquals;
import static io.aeron.driver.media.NetworkUtil.formatAddressAndPort;
import static io.aeron.protocol.ResolutionEntryFlyweight.*;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Default {@link NameResolver} for the {@link MediaDriver}.
 */
final class DriverNameResolver implements AutoCloseable, UdpNameResolutionTransport.UdpFrameHandler, NameResolver
{
    static NameResolver bootstrapNameResolver = DefaultNameResolver.INSTANCE;
    private static final String RESOLVER_NEIGHBORS_COUNTER_LABEL = "Resolver neighbors";

    // TODO: make these configurable
    private static final long SELF_RESOLUTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long NEIGHBOR_RESOLUTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long WORK_INTERVAL_MS = 10;

    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final HeaderFlyweight headerFlyweight = new HeaderFlyweight(unsafeBuffer);
    private final ResolutionEntryFlyweight resolutionEntryFlyweight = new ResolutionEntryFlyweight();
    private final ArrayList<Neighbor> neighborList = new ArrayList<>();

    private final UdpNameResolutionTransport transport;
    private final DriverNameResolverCache cache;
    private final NameResolver delegateResolver;
    private final AtomicCounter invalidPackets;
    private final AtomicCounter shortSends;
    private final AtomicCounter neighborsCounter;
    private final AtomicCounter cacheEntriesCounter;
    private final byte[] nameTempBuffer = new byte[ResolutionEntryFlyweight.MAX_NAME_LENGTH];
    private final byte[] addressTempBuffer = new byte[ResolutionEntryFlyweight.ADDRESS_LENGTH_IP6];

    private final String localDriverName;
    private InetSocketAddress localSocketAddress;
    private final byte[] localName;
    private byte[] localAddress;

    private final String[] bootstrapNeighbors;
    private InetSocketAddress bootstrapNeighborAddress;
    private long bootstrapNeighborResolveDeadlineMs;

    private final long neighborTimeoutMs = TIMEOUT_MS;
    private final long selfResolutionIntervalMs = SELF_RESOLUTION_INTERVAL_MS;
    private final long neighborResolutionIntervalMs = NEIGHBOR_RESOLUTION_INTERVAL_MS;
    private final int mtuLength;
    private final boolean preferIPv6 = false;

    private long workDeadlineMs = 0;
    private long selfResolutionDeadlineMs;
    private long neighborResolutionDeadlineMs;

    DriverNameResolver(final MediaDriver.Context ctx)
    {
        mtuLength = ctx.mtuLength();
        invalidPackets = ctx.systemCounters().get(SystemCounterDescriptor.INVALID_PACKETS);
        shortSends = ctx.systemCounters().get(SystemCounterDescriptor.SHORT_SENDS);
        delegateResolver = ctx.nameResolver();

        final long nowMs = ctx.epochClock().time();

        bootstrapNeighbors = null != ctx.resolverBootstrapNeighbor() ?
            ctx.resolverBootstrapNeighbor().split(",") : null;
        bootstrapNeighborAddress = null != bootstrapNeighbors ? resolveBootstrapNeighbor() : null;
        bootstrapNeighborResolveDeadlineMs = nowMs + TIMEOUT_MS;

        localSocketAddress = null != ctx.resolverInterface() ?
            UdpNameResolutionTransport.getInterfaceAddress(ctx.resolverInterface()) :
            new InetSocketAddress("0.0.0.0", 0);

        localDriverName = null != ctx.resolverName() ? ctx.resolverName() : getCanonicalName();
        localName = localDriverName.getBytes(StandardCharsets.US_ASCII);
        localAddress = localSocketAddress.getAddress().getAddress();

        selfResolutionDeadlineMs = 0;
        neighborResolutionDeadlineMs = nowMs + neighborResolutionIntervalMs;

        cache = new DriverNameResolverCache(TIMEOUT_MS);

        final UdpChannel placeholderChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:8050");
        transport = new UdpNameResolutionTransport(placeholderChannel, localSocketAddress, unsafeBuffer, ctx);

        neighborsCounter = ctx.countersManager().newCounter(
            RESOLVER_NEIGHBORS_COUNTER_LABEL, AeronCounters.NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID);
        cacheEntriesCounter = ctx.countersManager().newCounter(
            "Resolver cache entries: name=" + localDriverName,
            AeronCounters.NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID);

        openDatagramChannel();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.closeAll(transport, cache);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > workDeadlineMs)
        {
            workDeadlineMs = nowMs + WORK_INTERVAL_MS;
            workCount += transport.poll(this, nowMs);
            workCount += cache.timeoutOldEntries(nowMs, cacheEntriesCounter);
            workCount += timeoutNeighbors(nowMs);

            if (nowMs > selfResolutionDeadlineMs)
            {
                sendSelfResolutions(nowMs);
            }

            if (nowMs > neighborResolutionDeadlineMs)
            {
                sendNeighborResolutions(nowMs);
            }
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        DriverNameResolverCache.CacheEntry entry;

        if (preferIPv6)
        {
            entry = cache.lookup(name, RES_TYPE_NAME_TO_IP6_MD);
            if (null == entry)
            {
                entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4_MD);
            }
        }
        else
        {
            entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4_MD);
        }

        InetAddress resolvedAddress = null;
        try
        {
            if (null == entry)
            {
                if (name.equals(localDriverName))
                {
                    return localSocketAddress.getAddress();
                }

                return delegateResolver.resolve(name, uriParamName, isReResolution);
            }

            resolvedAddress = InetAddress.getByAddress(entry.address);
        }
        catch (final UnknownHostException ignore)
        {
        }

        DefaultNameResolver.INSTANCE.resolveHook(this.getClass().getSimpleName(), name, resolvedAddress);

        return resolvedAddress;
    }

    /**
     * {@inheritDoc}
     */
    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        // here we would lookup advertised endpoints/control IP:port pairs by name. Currently, we just return delegate.
        return delegateResolver.lookup(name, uriParamName, isReLookup);
    }

    /**
     * {@inheritDoc}
     */
    public int onFrame(
        final UnsafeBuffer unsafeBuffer, final int length, final InetSocketAddress srcAddress, final long nowMs)
    {
        if (headerFlyweight.headerType() == HDR_TYPE_RES)
        {
            int offset = MIN_HEADER_LENGTH;

            while (length > offset)
            {
                resolutionEntryFlyweight.wrap(unsafeBuffer, offset, length - offset);

                if ((length - offset) < resolutionEntryFlyweight.entryLength())
                {
                    invalidPackets.increment();
                    return 0;
                }

                onResolutionEntry(resolutionEntryFlyweight, srcAddress, nowMs);

                offset += resolutionEntryFlyweight.entryLength();
            }

            return length;
        }

        return 0;
    }

    static String getCanonicalName()
    {
        String canonicalName = null;

        try
        {
            canonicalName = InetAddress.getLocalHost().getHostName();
        }
        catch (final UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return canonicalName;
    }

    static String getCanonicalName(final String fallback)
    {
        try
        {
            return InetAddress.getLocalHost().getHostName();
        }
        catch (final UnknownHostException ignore)
        {
            return fallback;
        }
    }


    private void openDatagramChannel()
    {
        transport.openDatagramChannel(null);

        final InetSocketAddress boundAddress = transport.boundAddress();
        if (null != boundAddress)
        {
            localSocketAddress = boundAddress;
            localAddress = boundAddress.getAddress().getAddress();

            neighborsCounter.appendToLabel(buildNeighborsCounterLabel(""));
        }
    }

    private String buildNeighborsCounterLabel(final String prefix)
    {
        final StringBuilder builder = new StringBuilder(prefix);
        builder.append(": bound ").append(transport.bindAddressAndPort());

        if (null != bootstrapNeighborAddress)
        {
            builder.append(" bootstrap ").append(
                formatAddressAndPort(bootstrapNeighborAddress.getAddress(), bootstrapNeighborAddress.getPort()));
        }

        return builder.toString();
    }

    private int timeoutNeighbors(final long nowMs)
    {
        int workCount = 0;

        final ArrayList<Neighbor> neighborList = this.neighborList;
        for (int lastIndex = neighborList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Neighbor neighbor = neighborList.get(i);

            if (nowMs > (neighbor.timeOfLastActivityMs + neighborTimeoutMs))
            {
                Neighbor.neighbourRemoved(nowMs, neighbor.socketAddress);
                ArrayListUtil.fastUnorderedRemove(neighborList, i, lastIndex--);
                workCount++;
            }
        }

        final int neighborCount = neighborList.size();
        if (neighborsCounter.getWeak() != neighborCount)
        {
            neighborsCounter.setOrdered(neighborCount);
        }

        return workCount;
    }

    private void sendSelfResolutions(final long nowMs)
    {
        byteBuffer.clear();

        final int currentOffset = HeaderFlyweight.MIN_HEADER_LENGTH;
        final byte resType = preferIPv6 ? RES_TYPE_NAME_TO_IP6_MD : RES_TYPE_NAME_TO_IP4_MD;

        headerFlyweight
            .headerType(HeaderFlyweight.HDR_TYPE_RES)
            .flags((short)0)
            .version(HeaderFlyweight.CURRENT_VERSION);

        resolutionEntryFlyweight.wrap(unsafeBuffer, currentOffset, unsafeBuffer.capacity() - currentOffset);
        resolutionEntryFlyweight
            .resType(resType)
            .flags(SELF_FLAG)
            .udpPort((short)localSocketAddress.getPort())
            .ageInMs(0)
            .putAddress(localAddress)
            .putName(localName);

        final int length = resolutionEntryFlyweight.entryLength() + MIN_HEADER_LENGTH;
        headerFlyweight.frameLength(length);

        byteBuffer.limit(length);

        boolean sendToBootstrap = null != bootstrapNeighborAddress;
        for (final Neighbor neighbor : neighborList)
        {
            sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);

            if (sendToBootstrap && neighbor.socketAddress.equals(bootstrapNeighborAddress))
            {
                sendToBootstrap = false;
            }
        }

        if (sendToBootstrap)
        {
            if (nowMs > bootstrapNeighborResolveDeadlineMs)
            {
                final InetSocketAddress oldAddress = this.bootstrapNeighborAddress;
                bootstrapNeighborAddress = resolveBootstrapNeighbor();
                bootstrapNeighborResolveDeadlineMs = nowMs + TIMEOUT_MS;

                if (!oldAddress.equals(bootstrapNeighborAddress))
                {
                    neighborsCounter.updateLabel(buildNeighborsCounterLabel(RESOLVER_NEIGHBORS_COUNTER_LABEL));

                    // avoid sending resolution frame if new bootstrap is in the neighbors list
                    for (final Neighbor neighbor : neighborList)
                    {
                        if (neighbor.socketAddress.equals(bootstrapNeighborAddress))
                        {
                            sendToBootstrap = false;
                            break;
                        }
                    }
                }
            }

            if (sendToBootstrap)
            {
                sendResolutionFrameTo(byteBuffer, bootstrapNeighborAddress);
            }
        }

        selfResolutionDeadlineMs = nowMs + selfResolutionIntervalMs;
    }

    private void sendResolutionFrameTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        buffer.position(0);

        final int bytesRemaining = buffer.remaining();
        final int bytesSent = transport.sendTo(buffer, remoteAddress);

        if (0 <= bytesSent && bytesSent < bytesRemaining)
        {
            shortSends.increment();
        }
    }

    private void onResolutionEntry(
        final ResolutionEntryFlyweight resolutionEntry, final InetSocketAddress srcAddress, final long nowMs)
    {
        final byte resType = resolutionEntry.resType();
        final boolean isSelf = SELF_FLAG == resolutionEntryFlyweight.flags();
        byte[] addr = addressTempBuffer;

        final int addressLength = resolutionEntryFlyweight.getAddress(addressTempBuffer);
        if (isSelf && ResolutionEntryFlyweight.isAnyLocalAddress(addressTempBuffer, addressLength))
        {
            addr = srcAddress.getAddress().getAddress();
        }

        final int nameLength = resolutionEntryFlyweight.getName(nameTempBuffer);
        final long timeOfLastActivity = nowMs - resolutionEntryFlyweight.ageInMs();
        final int port = resolutionEntryFlyweight.udpPort();

        // use name and port to indicate it is from this resolver instead of searching interfaces
        if (port == localSocketAddress.getPort() && byteSubsetEquals(nameTempBuffer, localName, nameLength))
        {
            return;
        }

        cache.addOrUpdateEntry(
            nameTempBuffer, nameLength, timeOfLastActivity, resType, addr, port, cacheEntriesCounter);

        final int neighborIndex = findNeighborByAddress(addr, addressLength, port);
        if (-1 == neighborIndex)
        {
            final byte[] neighborAddress = Arrays.copyOf(addr, addressLength);

            try
            {
                final Neighbor neighbor = new Neighbor(new InetSocketAddress(
                    InetAddress.getByAddress(neighborAddress), port), timeOfLastActivity);
                Neighbor.neighbourAdded(nowMs, neighbor.socketAddress);
                neighborList.add(neighbor);
                neighborsCounter.setOrdered(neighborList.size());
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else if (isSelf)
        {
            neighborList.get(neighborIndex).timeOfLastActivityMs = timeOfLastActivity;
        }
    }

    private int findNeighborByAddress(final byte[] address, final int addressLength, final int port)
    {
        for (int i = 0, size = neighborList.size(); i < size; i++)
        {
            final InetSocketAddress socketAddress = neighborList.get(i).socketAddress;

            if (byteSubsetEquals(address, socketAddress.getAddress().getAddress(), addressLength) &&
                port == socketAddress.getPort())
            {
                return i;
            }
        }

        return -1;
    }

    private void sendNeighborResolutions(final long nowMs)
    {
        for (final DriverNameResolverCache.Iterator iter = cache.resetIterator(); iter.hasNext(); )
        {
            byteBuffer.clear();
            headerFlyweight
                .headerType(HeaderFlyweight.HDR_TYPE_RES)
                .flags((short)0)
                .version(HeaderFlyweight.CURRENT_VERSION);

            int currentOffset = HeaderFlyweight.MIN_HEADER_LENGTH;

            while (iter.hasNext())
            {
                final DriverNameResolverCache.CacheEntry entry = iter.next();

                if (currentOffset + entryLengthRequired(entry.type, entry.name.length) > mtuLength)
                {
                    iter.rewindNext();
                    break;
                }

                resolutionEntryFlyweight.wrap(unsafeBuffer, currentOffset, unsafeBuffer.capacity() - currentOffset);
                resolutionEntryFlyweight
                    .resType(entry.type)
                    .flags((short)0)
                    .udpPort((short)entry.port)
                    .ageInMs((int)(nowMs - entry.timeOfLastActivityMs))
                    .putAddress(entry.address)
                    .putName(entry.name);

                final int length = resolutionEntryFlyweight.entryLength();
                currentOffset += length;
            }

            headerFlyweight.frameLength(currentOffset);
            byteBuffer.limit(currentOffset);

            for (int i = 0, size = neighborList.size(); i < size; i++)
            {
                final Neighbor neighbor = neighborList.get(i);
                sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);
            }
        }

        neighborResolutionDeadlineMs = nowMs + neighborResolutionIntervalMs;
    }

    private InetSocketAddress resolveBootstrapNeighbor()
    {
        Throwable t = null;
        for (final String neighbor : bootstrapNeighbors)
        {
            try
            {
                return UdpNameResolutionTransport.getInetSocketAddress(neighbor, bootstrapNameResolver);
            }
            catch (final Throwable ex)
            {
                if (null == t)
                {
                    t = ex;
                }
                else
                {
                    t.addSuppressed(ex);
                }
            }
        }

        if (null != t)
        {
            LangUtil.rethrowUnchecked(t);
        }

        return null;
    }

    static class Neighbor
    {
        final InetSocketAddress socketAddress;
        long timeOfLastActivityMs;

        Neighbor(final InetSocketAddress socketAddress, final long nowMs)
        {
            this.socketAddress = socketAddress;
            this.timeOfLastActivityMs = nowMs;
        }

        static void neighbourAdded(final long nowMs, final InetSocketAddress address)
        {
//            System.out.println(nowMs + " neighbour added: " + address);
        }

        static void neighbourRemoved(final long nowMs, final InetSocketAddress address)
        {
//            System.out.println(nowMs + " neighbour removed: " + address);
        }
    }
}
