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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.buffer.LogFactory;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.*;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.*;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.*;
import static io.aeron.CommonContext.InferableBoolean.FORCE_TRUE;
import static io.aeron.CommonContext.InferableBoolean.INFER;
import static io.aeron.ErrorCode.*;
import static io.aeron.driver.PublicationParams.*;
import static io.aeron.driver.SubscriptionParams.validateInitialWindowForRcvBuf;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static org.agrona.collections.ArrayListUtil.fastUnorderedRemove;

/**
 * Driver Conductor that takes commands from publishers and subscribers, and orchestrates the media driver.
 */
public final class DriverConductor implements Agent
{
    private static final long CLOCK_UPDATE_INTERNAL_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final String[] INVALID_DESTINATION_KEYS =
    {
        MTU_LENGTH_PARAM_NAME,
        RECEIVER_WINDOW_LENGTH_PARAM_NAME,
        SOCKET_RCVBUF_PARAM_NAME,
        SOCKET_SNDBUF_PARAM_NAME
    };

    private int nextSessionId = BitUtil.generateRandomisedId();
    private final long timerIntervalNs;
    private final long clientLivenessTimeoutNs;
    private long timeOfLastToDriverPositionChangeNs;
    private long lastConsumerCommandPosition;
    private long timerCheckDeadlineNs;
    private long clockUpdateDeadlineNs;

    private final Context ctx;
    private final LogFactory logFactory;
    private final ReceiverProxy receiverProxy;
    private final SenderProxy senderProxy;
    private final ClientProxy clientProxy;
    private final RingBuffer toDriverCommands;
    private final ClientCommandAdapter clientCommandAdapter;
    private final ManyToOneConcurrentArrayQueue<Runnable> driverCmdQueue;
    private final Object2ObjectHashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap =
        new Object2ObjectHashMap<>();
    private final Object2ObjectHashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap =
        new Object2ObjectHashMap<>();
    private final ArrayList<NetworkPublication> networkPublications = new ArrayList<>();
    private final ArrayList<IpcPublication> ipcPublications = new ArrayList<>();
    private final ArrayList<PublicationImage> publicationImages = new ArrayList<>();
    private final ArrayList<PublicationLink> publicationLinks = new ArrayList<>();
    private final ArrayList<SubscriptionLink> subscriptionLinks = new ArrayList<>();
    private final ArrayList<CounterLink> counterLinks = new ArrayList<>();
    private final ArrayList<AeronClient> clients = new ArrayList<>();
    private final ObjectHashSet<SessionKey> activeSessionSet = new ObjectHashSet<>();
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final CachedEpochClock cachedEpochClock;
    private final CachedNanoClock cachedNanoClock;
    private final CountersManager countersManager;
    private final NetworkPublicationThreadLocals networkPublicationThreadLocals = new NetworkPublicationThreadLocals();
    private final MutableDirectBuffer tempBuffer;
    private final DataHeaderFlyweight defaultDataHeader = new DataHeaderFlyweight(createDefaultHeader(0, 0, 0));
    private NameResolver nameResolver;
    private DriverNameResolver driverNameResolver;
    private final AtomicCounter errorCounter;
    private final AtomicCounter maxCycleTime;
    private final AtomicCounter cycleTimeThresholdExceededCount;

    DriverConductor(final MediaDriver.Context ctx)
    {
        this.ctx = ctx;
        timerIntervalNs = ctx.timerIntervalNs();
        clientLivenessTimeoutNs = ctx.clientLivenessTimeoutNs();
        driverCmdQueue = ctx.driverCommandQueue();
        receiverProxy = ctx.receiverProxy();
        senderProxy = ctx.senderProxy();
        logFactory = ctx.logFactory();
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        cachedEpochClock = ctx.cachedEpochClock();
        cachedNanoClock = ctx.cachedNanoClock();
        toDriverCommands = ctx.toDriverCommands();
        clientProxy = ctx.clientProxy();
        tempBuffer = ctx.tempBuffer();
        errorCounter = ctx.systemCounters().get(ERRORS);

        countersManager = ctx.countersManager();

        clientCommandAdapter = new ClientCommandAdapter(
            errorCounter,
            ctx.errorHandler(),
            toDriverCommands,
            clientProxy,
            this);

        lastConsumerCommandPosition = toDriverCommands.consumerPosition();
        maxCycleTime = ctx.systemCounters().get(CONDUCTOR_MAX_CYCLE_TIME);
        cycleTimeThresholdExceededCount = ctx.systemCounters().get(CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED);
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        if (null == ctx.resolverInterface())
        {
            driverNameResolver = null;
            nameResolver = ctx.nameResolver();
        }
        else
        {
            driverNameResolver = new DriverNameResolver(ctx);
            nameResolver = driverNameResolver;
        }

        ctx.systemCounters().get(RESOLUTION_CHANGES).appendToLabel(
            ": driverName=" + ctx.resolverName() +
            " hostname=" + DriverNameResolver.getCanonicalName("<unresolved>"));

        ctx.systemCounters().get(CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED).appendToLabel(
            ": threshold=" + ctx.conductorCycleThresholdNs() + "ns");

        nameResolver.init(ctx);

        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        cachedEpochClock.update(epochClock.time());
        timerCheckDeadlineNs = nowNs + timerIntervalNs;
        clockUpdateDeadlineNs = nowNs + CLOCK_UPDATE_INTERNAL_NS;
        timeOfLastToDriverPositionChangeNs = nowNs;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        CloseHelper.close(ctx.errorHandler(), driverNameResolver);
        publicationImages.forEach(PublicationImage::free);
        networkPublications.forEach(NetworkPublication::free);
        ipcPublications.forEach(IpcPublication::free);
        toDriverCommands.consumerHeartbeatTime(Aeron.NULL_VALUE);
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "driver-conductor";
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        trackTime(nowNs);

        int workCount = 0;
        workCount += processTimers(nowNs);
        workCount += clientCommandAdapter.receive();
        workCount += driverCmdQueue.drain(Runnable::run, Configuration.COMMAND_DRAIN_LIMIT);
        workCount += trackStreamPositions(workCount, nowNs);
        workCount += nameResolver.doWork(cachedEpochClock.time());

        return workCount;
    }

    void onCreatePublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int initialTermOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        final UdpChannel subscriptionChannel = channelEndpoint.subscriptionUdpChannel();

        Configuration.validateMtuLength(senderMtuLength);
        Configuration.validateInitialWindowLength(
            subscriptionChannel.receiverWindowLengthOrDefault(ctx.initialWindowLength()), senderMtuLength);

        final long joinPosition = computePosition(
            activeTermId, initialTermOffset, LogBufferDescriptor.positionBitsToShift(termBufferLength), initialTermId);
        final ArrayList<SubscriberPosition> subscriberPositions = createSubscriberPositions(
            sessionId, streamId, channelEndpoint, joinPosition);

        if (subscriberPositions.size() > 0)
        {
            RawLog rawLog = null;
            CongestionControl congestionControl = null;
            UnsafeBufferPosition hwmPos = null;
            UnsafeBufferPosition rcvPos = null;

            try
            {
                final long registrationId = toDriverCommands.nextCorrelationId();
                rawLog = newPublicationImageLog(
                    sessionId,
                    streamId,
                    initialTermId,
                    termBufferLength,
                    isOldestSubscriptionSparse(subscriberPositions),
                    senderMtuLength,
                    registrationId);

                congestionControl = ctx.congestionControlSupplier().newInstance(
                    registrationId,
                    subscriptionChannel,
                    streamId,
                    sessionId,
                    termBufferLength,
                    senderMtuLength,
                    controlAddress,
                    sourceAddress,
                    ctx.receiverCachedNanoClock(),
                    ctx,
                    countersManager);

                final String uri = subscriptionChannel.originalUriString();
                hwmPos = ReceiverHwm.allocate(tempBuffer, countersManager, registrationId, sessionId, streamId, uri);
                rcvPos = ReceiverPos.allocate(tempBuffer, countersManager, registrationId, sessionId, streamId, uri);

                final UdpChannel destChannel = channelEndpoint.udpChannel();
                final InferableBoolean groupSubscription = subscriberPositions.get(0).subscription().group();
                final boolean treatAsMulticast = groupSubscription == INFER ?
                    destChannel.isMulticast() : groupSubscription == FORCE_TRUE;

                final PublicationImage image = new PublicationImage(
                    registrationId,
                    ctx,
                    channelEndpoint,
                    transportIndex,
                    controlAddress,
                    sessionId,
                    streamId,
                    initialTermId,
                    activeTermId,
                    initialTermOffset,
                    rawLog,
                    treatAsMulticast ? ctx.multicastFeedbackDelayGenerator() : ctx.unicastFeedbackDelayGenerator(),
                    subscriberPositions,
                    hwmPos,
                    rcvPos,
                    sourceAddress,
                    congestionControl);

                publicationImages.add(image);
                receiverProxy.newPublicationImage(channelEndpoint, image);

                final String sourceIdentity = Configuration.sourceIdentity(sourceAddress);
                for (int i = 0, size = subscriberPositions.size(); i < size; i++)
                {
                    final SubscriberPosition position = subscriberPositions.get(i);
                    position.addLink(image);

                    clientProxy.onAvailableImage(
                        registrationId,
                        streamId,
                        sessionId,
                        position.subscription().registrationId(),
                        position.positionCounterId(),
                        rawLog.fileName(),
                        sourceIdentity);
                }
            }
            catch (final Throwable ex)
            {
                subscriberPositions.forEach((subscriberPosition) -> subscriberPosition.position().close());
                CloseHelper.quietCloseAll(rawLog, congestionControl, hwmPos, rcvPos);
                throw ex;
            }
        }
    }

    void onChannelEndpointError(final long statusIndicatorId, final Exception ex)
    {
        final String errorMessage = ex.getClass().getName() + " : " + ex.getMessage();
        clientProxy.onError(statusIndicatorId, CHANNEL_ENDPOINT_ERROR, errorMessage);
    }

    void onReResolveEndpoint(
        final String endpoint, final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        try
        {
            final InetSocketAddress newAddress = UdpChannel.resolve(
                endpoint, CommonContext.ENDPOINT_PARAM_NAME, true, nameResolver);

            if (newAddress.isUnresolved())
            {
                ctx.errorHandler().onError(new AeronEvent("could not re-resolve: endpoint=" + endpoint));
                errorCounter.increment();
            }
            else if (!address.equals(newAddress))
            {
                senderProxy.onResolutionChange(channelEndpoint, endpoint, newAddress);
            }
        }
        catch (final Throwable ex)
        {
            ctx.errorHandler().onError(ex);
            errorCounter.increment();
        }
    }

    void onReResolveControl(
        final String control,
        final UdpChannel udpChannel,
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress address)
    {
        try
        {
            final InetSocketAddress newAddress = UdpChannel.resolve(
                control, CommonContext.MDC_CONTROL_PARAM_NAME, true, nameResolver);

            if (newAddress.isUnresolved())
            {
                ctx.errorHandler().onError(new AeronEvent("could not re-resolve: control=" + control));
                errorCounter.increment();
            }
            else if (!address.equals(newAddress))
            {
                receiverProxy.onResolutionChange(channelEndpoint, udpChannel, newAddress);
            }
        }
        catch (final Throwable ex)
        {
            ctx.errorHandler().onError(ex);
            errorCounter.increment();
        }
    }

    IpcPublication getSharedIpcPublication(final long streamId)
    {
        return findSharedIpcPublication(ipcPublications, streamId);
    }

    IpcPublication getIpcPublication(final long registrationId)
    {
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.registrationId() == registrationId)
            {
                return publication;
            }
        }

        return null;
    }

    NetworkPublication findNetworkPublicationByTag(final long tag)
    {
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);
            final long publicationTag = publication.tag();
            if (publicationTag == tag && publicationTag != ChannelUri.INVALID_TAG)
            {
                return publication;
            }
        }

        return null;
    }

    IpcPublication findIpcPublicationByTag(final long tag)
    {
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            final long publicationTag = publication.tag();
            if (publicationTag == tag && publicationTag != ChannelUri.INVALID_TAG)
            {
                return publication;
            }
        }

        return null;
    }

    void onAddNetworkPublication(
        final String channel,
        final int streamId,
        final long correlationId,
        final long clientId,
        final boolean isExclusive)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel, nameResolver);
        final ChannelUri channelUri = udpChannel.channelUri();
        final PublicationParams params = getPublicationParams(channelUri, ctx, this, isExclusive, false);
        validateMtuForMaxMessage(params);

        final SendChannelEndpoint channelEndpoint = getOrCreateSendChannelEndpoint(params, udpChannel, correlationId);

        NetworkPublication publication = null;
        if (!isExclusive)
        {
            publication = findPublication(networkPublications, streamId, channelEndpoint);
        }

        if (null == publication)
        {
            if (params.hasSessionId)
            {
                checkForSessionClash(params.sessionId, streamId, udpChannel.canonicalForm());
            }

            publication = newNetworkPublication(
                correlationId, clientId, streamId, channel, udpChannel, channelEndpoint, params, isExclusive);
        }
        else
        {
            confirmMatch(channelUri, params, publication.rawLog(), publication.sessionId());
            validateSpiesSimulateConnection(params, publication.spiesSimulateConnection());
        }

        publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), publication));

        clientProxy.onPublicationReady(
            correlationId,
            publication.registrationId(),
            streamId,
            publication.sessionId(),
            publication.rawLog().fileName(),
            publication.publisherLimitId(),
            channelEndpoint.statusIndicatorCounterId(),
            isExclusive);
    }

    void cleanupSpies(final NetworkPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                clientProxy.onUnavailableImage(
                    publication.registrationId(), link.registrationId(), publication.streamId(), publication.channel());
                subscriptionLinks.get(i).unlink(publication);
            }
        }
    }

    void notifyUnavailableImageLink(final long resourceId, final SubscriptionLink link)
    {
        clientProxy.onUnavailableImage(resourceId, link.registrationId(), link.streamId(), link.channel());
    }

    void notifyAvailableImageLink(
        final long resourceId,
        final int sessionId,
        final SubscriptionLink link,
        final int positionCounterId,
        final long joinPosition,
        final String logFileName,
        final String sourceIdentity)
    {
        countersManager.setCounterValue(positionCounterId, joinPosition);

        final int streamId = link.streamId();
        clientProxy.onAvailableImage(
            resourceId, streamId, sessionId, link.registrationId(), positionCounterId, logFileName, sourceIdentity);
    }

    void cleanupPublication(final NetworkPublication publication)
    {
        senderProxy.removeNetworkPublication(publication);

        final SendChannelEndpoint channelEndpoint = publication.channelEndpoint();
        if (channelEndpoint.shouldBeClosed())
        {
            senderProxy.closeSendChannelEndpoint(channelEndpoint);
            sendChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
            channelEndpoint.closeIndicators();
        }

        final String channel = channelEndpoint.udpChannel().canonicalForm();
        activeSessionSet.remove(new SessionKey(publication.sessionId(), publication.streamId(), channel));
    }

    void cleanupSubscriptionLink(final SubscriptionLink subscription)
    {
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();

        if (null != channelEndpoint)
        {
            if (subscription.hasSessionId())
            {
                if (0 == channelEndpoint.decRefToStreamAndSession(subscription.streamId(), subscription.sessionId()))
                {
                    receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
                }
            }
            else
            {
                if (0 == channelEndpoint.decRefToStream(subscription.streamId()))
                {
                    receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
                }
            }

            if (channelEndpoint.shouldBeClosed())
            {
                receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);
                receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                channelEndpoint.closeIndicators();
            }
        }
    }

    void transitionToLinger(final PublicationImage image)
    {
        boolean rejoin = true;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(image))
            {
                rejoin = link.isRejoin();
                clientProxy.onUnavailableImage(
                    image.correlationId(), link.registrationId(), image.streamId(), image.channel());
            }
        }

        if (rejoin)
        {
            receiverProxy.removeCoolDown(image.channelEndpoint(), image.sessionId(), image.streamId());
        }
    }

    void transitionToLinger(final IpcPublication publication)
    {
        activeSessionSet.remove(new SessionKey(publication.sessionId(), publication.streamId(), IPC_MEDIA));

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                clientProxy.onUnavailableImage(
                    publication.registrationId(),
                    link.registrationId(),
                    publication.streamId(),
                    CommonContext.IPC_CHANNEL);
            }
        }
    }

    void cleanupImage(final PublicationImage image)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(image);
        }
    }

    void cleanupIpcPublication(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(publication);
        }
    }

    void clientTimeout(final long clientId)
    {
        clientProxy.onClientTimeout(clientId);
    }

    void unavailableCounter(final long registrationId, final int counterId)
    {
        clientProxy.onUnavailableCounter(registrationId, counterId);
    }

    void onAddIpcPublication(
        final String channel,
        final int streamId,
        final long correlationId,
        final long clientId,
        final boolean isExclusive)
    {
        final IpcPublication ipcPublication = getOrAddIpcPublication(
            correlationId, clientId, streamId, channel, isExclusive);
        publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), ipcPublication));

        final ArrayList<SubscriberPosition> subscriberPositions = linkIpcSubscriptions(ipcPublication);

        clientProxy.onPublicationReady(
            correlationId,
            ipcPublication.registrationId(),
            streamId,
            ipcPublication.sessionId(),
            ipcPublication.rawLog().fileName(),
            ipcPublication.publisherLimitId(),
            ChannelEndpointStatus.NO_ID_ALLOCATED,
            isExclusive);

        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            final SubscriberPosition subscriberPosition = subscriberPositions.get(i);

            clientProxy.onAvailableImage(
                ipcPublication.registrationId(),
                streamId,
                ipcPublication.sessionId(),
                subscriberPosition.subscription().registrationId,
                subscriberPosition.position().id(),
                ipcPublication.rawLog().fileName(),
                CommonContext.IPC_CHANNEL);
        }
    }

    void onRemovePublication(final long registrationId, final long correlationId)
    {
        PublicationLink publicationLink = null;
        final ArrayList<PublicationLink> publicationLinks = this.publicationLinks;
        for (int i = 0, size = publicationLinks.size(); i < size; i++)
        {
            final PublicationLink publication = publicationLinks.get(i);
            if (registrationId == publication.registrationId())
            {
                publicationLink = publication;
                fastUnorderedRemove(publicationLinks, i);
                break;
            }
        }

        if (null == publicationLink)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        publicationLink.close();
        clientProxy.operationSucceeded(correlationId);
    }

    void onAddSendDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        final ChannelUri channelUri = ChannelUri.parse(destinationChannel);
        validateDestinationUri(channelUri, destinationChannel);

        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        final InetSocketAddress dstAddress = UdpChannel.destinationAddress(channelUri, nameResolver);
        senderProxy.addDestination(sendChannelEndpoint, channelUri, dstAddress);
        clientProxy.operationSucceeded(correlationId);
    }

    void onRemoveSendDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        final ChannelUri channelUri = ChannelUri.parse(destinationChannel);
        final InetSocketAddress dstAddress = UdpChannel.destinationAddress(channelUri, nameResolver);
        senderProxy.removeDestination(sendChannelEndpoint, channelUri, dstAddress);
        clientProxy.operationSucceeded(correlationId);
    }

    void onAddNetworkSubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel, nameResolver);

        validateTimestampConfiguration(udpChannel);

        final SubscriptionParams params = SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx);

        checkForClashingSubscription(params, udpChannel, streamId);
        final ReceiveChannelEndpoint channelEndpoint = getOrCreateReceiveChannelEndpoint(
            params, udpChannel, registrationId);

        if (params.hasSessionId)
        {
            if (1 == channelEndpoint.incRefToStreamAndSession(streamId, params.sessionId))
            {
                receiverProxy.addSubscription(channelEndpoint, streamId, params.sessionId);
            }
        }
        else
        {
            if (1 == channelEndpoint.incRefToStream(streamId))
            {
                receiverProxy.addSubscription(channelEndpoint, streamId);
            }
        }

        final AeronClient client = getOrAddClient(clientId);
        final NetworkSubscriptionLink subscription = new NetworkSubscriptionLink(
            registrationId, channelEndpoint, streamId, channel, client, params);

        subscriptionLinks.add(subscription);
        clientProxy.onSubscriptionReady(registrationId, channelEndpoint.statusIndicatorCounter().id());

        linkMatchingImages(subscription);
    }

    void onAddIpcSubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final SubscriptionParams params = SubscriptionParams.getSubscriptionParams(ChannelUri.parse(channel), ctx);
        final IpcSubscriptionLink subscriptionLink = new IpcSubscriptionLink(
            registrationId, streamId, channel, getOrAddClient(clientId), params);
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();

        subscriptionLinks.add(subscriptionLink);
        clientProxy.onSubscriptionReady(registrationId, ChannelEndpointStatus.NO_ID_ALLOCATED);

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
            {
                final Position subPos = linkIpcSubscription(publication, subscriptionLink);
                subscriberPositions.add(new SubscriberPosition(subscriptionLink, publication, subPos));
            }
        }

        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            final SubscriberPosition subscriberPosition = subscriberPositions.get(i);
            final IpcPublication publication = (IpcPublication)subscriberPosition.subscribable();

            clientProxy.onAvailableImage(
                publication.registrationId(),
                streamId,
                publication.sessionId(),
                registrationId,
                subscriberPosition.position().id(),
                publication.rawLog().fileName(),
                CommonContext.IPC_CHANNEL);
        }
    }

    void onAddSpySubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(channel, nameResolver);
        final AeronClient client = getOrAddClient(clientId);
        final SubscriptionParams params = SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx);
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();
        final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
            registrationId, udpChannel, streamId, client, params);

        subscriptionLinks.add(subscriptionLink);
        clientProxy.onSubscriptionReady(registrationId, ChannelEndpointStatus.NO_ID_ALLOCATED);

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);
            if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
            {
                final Position subPos = linkSpy(publication, subscriptionLink);
                subscriberPositions.add(new SubscriberPosition(subscriptionLink, publication, subPos));
            }
        }

        for (int i = 0, size = subscriberPositions.size(); i < size; i++)
        {
            final SubscriberPosition subscriberPosition = subscriberPositions.get(i);
            final NetworkPublication publication = (NetworkPublication)subscriberPosition.subscribable();

            clientProxy.onAvailableImage(
                publication.registrationId(),
                streamId,
                publication.sessionId(),
                registrationId,
                subscriberPosition.position().id(),
                publication.rawLog().fileName(),
                CommonContext.IPC_CHANNEL);
        }
    }

    void onRemoveSubscription(final long registrationId, final long correlationId)
    {
        final SubscriptionLink subscription = removeSubscriptionLink(subscriptionLinks, registrationId);
        if (null == subscription)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown subscription: " + registrationId);
        }

        subscription.close();
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();

        if (null != channelEndpoint)
        {
            if (subscription.hasSessionId())
            {
                if (0 == channelEndpoint.decRefToStreamAndSession(subscription.streamId(), subscription.sessionId()))
                {
                    receiverProxy.removeSubscription(
                        channelEndpoint, subscription.streamId(), subscription.sessionId());
                }
            }
            else
            {
                if (0 == channelEndpoint.decRefToStream(subscription.streamId()))
                {
                    receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
                }
            }

            if (channelEndpoint.shouldBeClosed())
            {
                receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);
                receiveChannelEndpointByChannelMap.remove(channelEndpoint.udpChannel().canonicalForm());
                channelEndpoint.closeIndicators();
            }
        }

        clientProxy.operationSucceeded(correlationId);
    }

    void onClientKeepalive(final long clientId)
    {
        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.timeOfLastKeepaliveMs(cachedEpochClock.time());
        }
    }

    void onAddCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long correlationId,
        final long clientId)
    {
        final AeronClient client = getOrAddClient(clientId);
        final AtomicCounter counter = countersManager.newCounter(
            typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

        countersManager.setCounterOwnerId(counter.id(), clientId);
        countersManager.setCounterRegistrationId(counter.id(), correlationId);
        counterLinks.add(new CounterLink(counter, correlationId, client));
        clientProxy.onCounterReady(correlationId, counter.id());
    }

    void onRemoveCounter(final long registrationId, final long correlationId)
    {
        CounterLink counterLink = null;
        final ArrayList<CounterLink> counterLinks = this.counterLinks;
        for (int i = 0, size = counterLinks.size(); i < size; i++)
        {
            final CounterLink link = counterLinks.get(i);
            if (registrationId == link.registrationId())
            {
                counterLink = link;
                fastUnorderedRemove(counterLinks, i);
                break;
            }
        }

        if (null == counterLink)
        {
            throw new ControlProtocolException(UNKNOWN_COUNTER, "unknown counter: " + registrationId);
        }

        clientProxy.operationSucceeded(correlationId);
        clientProxy.onUnavailableCounter(registrationId, counterLink.counterId());
        counterLink.close();
    }

    void onClientClose(final long clientId)
    {
        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.onClosedByCommand();
        }
    }

    void onAddRcvDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        final UdpChannel udpChannel = UdpChannel.parse(destinationChannel, nameResolver, true);
        validateDestinationUri(udpChannel.channelUri(), destinationChannel);

        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (registrationId == link.registrationId())
            {
                subscriptionLink = link;
                break;
            }
        }

        if (null == subscriptionLink)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown subscription: " + registrationId);
        }

        final ReceiveChannelEndpoint receiveChannelEndpoint = subscriptionLink.channelEndpoint();
        receiveChannelEndpoint.validateAllowsDestinationControl();

        final AtomicCounter localSocketAddressIndicator = ReceiveLocalSocketAddress.allocate(
            tempBuffer, countersManager, registrationId, receiveChannelEndpoint.statusIndicatorCounter().id());

        final ReceiveDestinationTransport transport = new ReceiveDestinationTransport(
            udpChannel, ctx, localSocketAddressIndicator, receiveChannelEndpoint);

        receiverProxy.addDestination(receiveChannelEndpoint, transport);
        clientProxy.operationSucceeded(correlationId);
    }

    void onRemoveRcvDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        ReceiveChannelEndpoint receiveChannelEndpoint = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
            if (registrationId == subscriptionLink.registrationId())
            {
                receiveChannelEndpoint = subscriptionLink.channelEndpoint();
                break;
            }
        }

        if (null == receiveChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown subscription: " + registrationId);
        }

        receiveChannelEndpoint.validateAllowsDestinationControl();
        receiverProxy.removeDestination(
            receiveChannelEndpoint, UdpChannel.parse(destinationChannel, nameResolver, true));
        clientProxy.operationSucceeded(correlationId);
    }

    void onTerminateDriver(final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        if (ctx.terminationValidator().allowTermination(ctx.aeronDirectory(), tokenBuffer, tokenOffset, tokenLength))
        {
            ctx.terminationHook().run();
        }
    }

    private void heartbeatAndCheckTimers(final long nowNs)
    {
        final long nowMs = cachedEpochClock.time();
        toDriverCommands.consumerHeartbeatTime(nowMs);

        checkManagedResources(clients, nowNs, nowMs);
        checkManagedResources(publicationLinks, nowNs, nowMs);
        checkManagedResources(networkPublications, nowNs, nowMs);
        checkManagedResources(subscriptionLinks, nowNs, nowMs);
        checkManagedResources(publicationImages, nowNs, nowMs);
        checkManagedResources(ipcPublications, nowNs, nowMs);
        checkManagedResources(counterLinks, nowNs, nowMs);
    }

    private void checkForBlockedToDriverCommands(final long nowNs)
    {
        final long consumerPosition = toDriverCommands.consumerPosition();

        if (consumerPosition == lastConsumerCommandPosition)
        {
            if (toDriverCommands.producerPosition() > consumerPosition &&
                ((timeOfLastToDriverPositionChangeNs + clientLivenessTimeoutNs) - nowNs < 0))
            {
                if (toDriverCommands.unblock())
                {
                    ctx.systemCounters().get(UNBLOCKED_COMMANDS).incrementOrdered();
                }
            }
        }
        else
        {
            timeOfLastToDriverPositionChangeNs = nowNs;
            lastConsumerCommandPosition = consumerPosition;
        }
    }

    private ArrayList<SubscriberPosition> createSubscriberPositions(
        final int sessionId, final int streamId, final ReceiveChannelEndpoint channelEndpoint, final long joinPosition)
    {
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(channelEndpoint, streamId, sessionId))
            {
                final Position position = SubscriberPos.allocate(
                    tempBuffer,
                    countersManager,
                    subscription.aeronClient().clientId(),
                    subscription.registrationId(),
                    sessionId,
                    streamId,
                    subscription.channel(),
                    joinPosition);

                position.setOrdered(joinPosition);
                subscriberPositions.add(new SubscriberPosition(subscription, null, position));
            }
        }

        return subscriberPositions;
    }

    private static NetworkPublication findPublication(
        final ArrayList<NetworkPublication> publications,
        final int streamId,
        final SendChannelEndpoint channelEndpoint)
    {
        for (int i = 0, size = publications.size(); i < size; i++)
        {
            final NetworkPublication publication = publications.get(i);

            if (streamId == publication.streamId() &&
                channelEndpoint == publication.channelEndpoint() &&
                NetworkPublication.State.ACTIVE == publication.state() &&
                !publication.isExclusive())
            {
                return publication;
            }
        }

        return null;
    }

    private NetworkPublication newNetworkPublication(
        final long registrationId,
        final long clientId,
        final int streamId,
        final String channel,
        final UdpChannel udpChannel,
        final SendChannelEndpoint channelEndpoint,
        final PublicationParams params,
        final boolean isExclusive)
    {
        final String canonicalForm = udpChannel.canonicalForm();
        final int sessionId = params.hasSessionId ? params.sessionId : nextAvailableSessionId(streamId, canonicalForm);
        final int initialTermId = params.hasPosition ? params.initialTermId : BitUtil.generateRandomisedId();

        final FlowControl flowControl = udpChannel.isMulticast() || udpChannel.isMultiDestination() ?
            ctx.multicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId) :
            ctx.unicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId);
        flowControl.initialize(ctx, udpChannel, initialTermId, params.termLength);

        final RawLog rawLog = newNetworkPublicationLog(sessionId, streamId, initialTermId, registrationId, params);
        UnsafeBufferPosition publisherPos = null;
        UnsafeBufferPosition publisherLmt = null;
        UnsafeBufferPosition senderPos = null;
        UnsafeBufferPosition senderLmt = null;
        AtomicCounter senderBpe = null;
        try
        {
            publisherPos = PublisherPos.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);
            publisherLmt = PublisherLimit.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);
            senderPos = SenderPos.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);
            senderLmt = SenderLimit.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);
            senderBpe = SenderBpe.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);

            countersManager.setCounterOwnerId(publisherLmt.id(), clientId);

            if (params.hasPosition)
            {
                final int bits = LogBufferDescriptor.positionBitsToShift(params.termLength);
                final long position = computePosition(params.termId, params.termOffset, bits, initialTermId);
                publisherPos.setOrdered(position);
                publisherLmt.setOrdered(position);
                senderPos.setOrdered(position);
                senderLmt.setOrdered(position);
            }

            final RetransmitHandler retransmitHandler = new RetransmitHandler(
                ctx.senderCachedNanoClock(),
                ctx.systemCounters().get(INVALID_PACKETS),
                ctx.retransmitUnicastDelayGenerator(),
                ctx.retransmitUnicastLingerGenerator());

            final NetworkPublication publication = new NetworkPublication(
                registrationId,
                ctx,
                params,
                channelEndpoint,
                rawLog,
                Configuration.producerWindowLength(params.termLength, ctx.publicationTermWindowLength()),
                publisherPos,
                publisherLmt,
                senderPos,
                senderLmt,
                senderBpe,
                sessionId,
                streamId,
                initialTermId,
                flowControl,
                retransmitHandler,
                networkPublicationThreadLocals,
                isExclusive);

            channelEndpoint.incRef();
            networkPublications.add(publication);
            senderProxy.newNetworkPublication(publication);
            linkSpies(subscriptionLinks, publication);
            activeSessionSet.add(new SessionKey(sessionId, streamId, canonicalForm));

            return publication;
        }
        catch (final Throwable ex)
        {
            CloseHelper.quietCloseAll(rawLog, publisherPos, publisherLmt, senderPos, senderLmt, senderBpe);
            throw ex;
        }
    }

    private RawLog newNetworkPublicationLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final long registrationId,
        final PublicationParams params)
    {
        final RawLog rawLog = logFactory.newPublication(registrationId, params.termLength, params.isSparse);
        initPublicationMetadata(sessionId, streamId, initialTermId, registrationId, params, rawLog);

        return rawLog;
    }

    private RawLog newIpcPublicationLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final long registrationId,
        final PublicationParams params)
    {
        final RawLog rawLog = logFactory.newPublication(registrationId, params.termLength, params.isSparse);
        initPublicationMetadata(sessionId, streamId, initialTermId, registrationId, params, rawLog);

        return rawLog;
    }

    private void initPublicationMetadata(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final long registrationId,
        final PublicationParams params,
        final RawLog rawLog)
    {
        final UnsafeBuffer logMetaData = rawLog.metaData();

        defaultDataHeader.sessionId(sessionId).streamId(streamId).termId(initialTermId);
        storeDefaultFrameHeader(logMetaData, defaultDataHeader);

        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, params.mtuLength);
        termLength(logMetaData, rawLog.termLength());
        pageSize(logMetaData, ctx.filePageSize());
        correlationId(logMetaData, registrationId);
        endOfStreamPosition(logMetaData, Long.MAX_VALUE);

        initialisePositionCounters(initialTermId, params, logMetaData);
    }

    private static void initialisePositionCounters(
        final int initialTermId, final PublicationParams params, final UnsafeBuffer logMetaData)
    {
        if (params.hasPosition)
        {
            final int termId = params.termId;
            final int termCount = termId - initialTermId;
            int activeIndex = indexByTerm(initialTermId, termId);

            rawTail(logMetaData, activeIndex, packTail(termId, params.termOffset));
            for (int i = 1; i < PARTITION_COUNT; i++)
            {
                final int expectedTermId = (termId + i) - PARTITION_COUNT;
                activeIndex = nextPartitionIndex(activeIndex);
                initialiseTailWithTermId(logMetaData, activeIndex, expectedTermId);
            }

            activeTermCount(logMetaData, termCount);
        }
        else
        {
            initialiseTailWithTermId(logMetaData, 0, initialTermId);
            for (int i = 1; i < PARTITION_COUNT; i++)
            {
                final int expectedTermId = (initialTermId + i) - PARTITION_COUNT;
                initialiseTailWithTermId(logMetaData, i, expectedTermId);
            }
        }
    }

    private RawLog newPublicationImageLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int termBufferLength,
        final boolean isSparse,
        final int senderMtuLength,
        final long correlationId)
    {
        final RawLog rawLog = logFactory.newImage(correlationId, termBufferLength, isSparse);
        final UnsafeBuffer logMetaData = rawLog.metaData();

        defaultDataHeader.sessionId(sessionId).streamId(streamId).termId(initialTermId);
        storeDefaultFrameHeader(logMetaData, defaultDataHeader);

        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, senderMtuLength);
        termLength(logMetaData, termBufferLength);
        pageSize(logMetaData, ctx.filePageSize());
        correlationId(logMetaData, correlationId);
        endOfStreamPosition(logMetaData, Long.MAX_VALUE);

        return rawLog;
    }

    private SendChannelEndpoint getOrCreateSendChannelEndpoint(
        final PublicationParams params, final UdpChannel udpChannel, final long registrationId)
    {
        SendChannelEndpoint channelEndpoint = findExistingSendChannelEndpoint(udpChannel);
        if (null == channelEndpoint)
        {
            AtomicCounter statusIndicator = null;
            AtomicCounter localSocketAddressIndicator = null;
            try
            {
                statusIndicator = SendChannelStatus.allocate(
                    tempBuffer, countersManager, registrationId, udpChannel.originalUriString());

                channelEndpoint = ctx.sendChannelEndpointSupplier().newInstance(udpChannel, statusIndicator, ctx);

                localSocketAddressIndicator = SendLocalSocketAddress.allocate(
                    tempBuffer, countersManager, registrationId, channelEndpoint.statusIndicatorCounterId());

                channelEndpoint.localSocketAddressIndicator(localSocketAddressIndicator);

                validateMtuForSndbuf(params, channelEndpoint.socketSndbufLength(), ctx);
                sendChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
                senderProxy.registerSendChannelEndpoint(channelEndpoint);
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(statusIndicator, localSocketAddressIndicator, channelEndpoint);
                throw ex;
            }
        }
        else
        {
            validateMtuForSndbuf(params, channelEndpoint.socketSndbufLength(), ctx);
            validateChannelBufferLength(
                SOCKET_RCVBUF_PARAM_NAME,
                udpChannel.socketRcvbufLength(),
                channelEndpoint.socketRcvbufLength(),
                udpChannel.originalUriString());
            validateChannelBufferLength(
                SOCKET_SNDBUF_PARAM_NAME,
                udpChannel.socketSndbufLength(),
                channelEndpoint.socketSndbufLength(),
                udpChannel.originalUriString());
        }

        return channelEndpoint;
    }

    private SendChannelEndpoint findExistingSendChannelEndpoint(final UdpChannel udpChannel)
    {
        if (udpChannel.hasTag())
        {
            for (final SendChannelEndpoint endpoint : sendChannelEndpointByChannelMap.values())
            {
                final UdpChannel endpointUdpChannel = endpoint.udpChannel();
                if (endpointUdpChannel.matchesTag(udpChannel))
                {
                    return endpoint;
                }
            }

            if (!udpChannel.hasExplicitControl() && !udpChannel.isManualControlMode() &&
                !udpChannel.channelUri().containsKey(CommonContext.ENDPOINT_PARAM_NAME))
            {
                throw new InvalidChannelException(
                    "URI must have explicit control, endpoint, or be manual control-mode when original: " +
                    udpChannel.originalUriString());
            }
        }

        SendChannelEndpoint endpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null != endpoint && endpoint.udpChannel().hasTag() && udpChannel.hasTag() &&
            endpoint.udpChannel().tag() != udpChannel.tag())
        {
            endpoint = null;
        }

        return endpoint;
    }

    private void checkForClashingSubscription(
        final SubscriptionParams params, final UdpChannel udpChannel, final int streamId)
    {
        final ReceiveChannelEndpoint channelEndpoint = findExistingReceiveChannelEndpoint(udpChannel);
        if (null != channelEndpoint)
        {
            for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
            {
                final SubscriptionLink subscription = subscriptionLinks.get(i);
                final boolean matchesTag = !udpChannel.hasTag() || channelEndpoint.matchesTag(udpChannel);

                if (matchesTag && subscription.matches(channelEndpoint, streamId, params))
                {
                    if (params.isReliable != subscription.isReliable())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: reliable=" + params.isReliable);
                    }

                    if (params.isRejoin != subscription.isRejoin())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: rejoin=" + params.isRejoin);
                    }
                }
            }
        }
    }

    private void linkMatchingImages(final SubscriptionLink subscriptionLink)
    {
        final long registrationId = subscriptionLink.registrationId();
        final long clientId = subscriptionLink.aeronClient().clientId();
        final int streamId = subscriptionLink.streamId();
        final String channel = subscriptionLink.channel();

        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            final PublicationImage image = publicationImages.get(i);
            if (subscriptionLink.matches(image) && image.isAcceptingSubscriptions())
            {
                final long joinPosition = image.joinPosition();
                final int sessionId = image.sessionId();
                final Position position = SubscriberPos.allocate(
                    tempBuffer,
                    countersManager,
                    clientId,
                    registrationId,
                    sessionId,
                    streamId,
                    channel,
                    joinPosition);

                position.setOrdered(joinPosition);
                subscriptionLink.link(image, position);
                image.addSubscriber(subscriptionLink, position, cachedNanoClock.nanoTime());

                clientProxy.onAvailableImage(
                    image.correlationId(),
                    streamId,
                    sessionId,
                    registrationId,
                    position.id(),
                    image.rawLog().fileName(),
                    Configuration.sourceIdentity(image.sourceAddress()));
            }
        }
    }

    private ArrayList<SubscriberPosition> linkIpcSubscriptions(final IpcPublication publication)
    {
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(publication) && !subscription.isLinked(publication))
            {
                final Position subPos = linkIpcSubscription(publication, subscription);
                subscriberPositions.add(new SubscriberPosition(subscription, publication, subPos));
            }
        }

        return subscriberPositions;
    }

    private Position linkIpcSubscription(final IpcPublication publication, final SubscriptionLink subscription)
    {
        final long joinPosition = publication.joinPosition();
        final long registrationId = subscription.registrationId();
        final long clientId = subscription.aeronClient().clientId();
        final int sessionId = publication.sessionId();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();

        final Position position = SubscriberPos.allocate(
            tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, channel, joinPosition);

        position.setOrdered(joinPosition);
        subscription.link(publication, position);
        publication.addSubscriber(subscription, position, cachedNanoClock.nanoTime());

        return position;
    }

    private Position linkSpy(final NetworkPublication publication, final SubscriptionLink subscription)
    {
        final long joinPosition = publication.consumerPosition();
        final long registrationId = subscription.registrationId();
        final long clientId = subscription.aeronClient().clientId();
        final int streamId = publication.streamId();
        final int sessionId = publication.sessionId();
        final String channel = subscription.channel();

        final Position position = SubscriberPos.allocate(
            tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, channel, joinPosition);

        position.setOrdered(joinPosition);
        subscription.link(publication, position);
        publication.addSubscriber(subscription, position, cachedNanoClock.nanoTime());

        return position;
    }

    private ReceiveChannelEndpoint getOrCreateReceiveChannelEndpoint(
        final SubscriptionParams params, final UdpChannel udpChannel, final long registrationId)
    {
        ReceiveChannelEndpoint channelEndpoint = findExistingReceiveChannelEndpoint(udpChannel);
        if (null == channelEndpoint)
        {
            AtomicCounter channelStatus = null;
            AtomicCounter localSocketAddressIndicator = null;
            try
            {
                final String channel = udpChannel.originalUriString();
                channelStatus = ReceiveChannelStatus.allocate(tempBuffer, countersManager, registrationId, channel);

                final DataPacketDispatcher dispatcher = new DataPacketDispatcher(
                    ctx.driverConductorProxy(), receiverProxy.receiver());
                channelEndpoint = ctx.receiveChannelEndpointSupplier().newInstance(
                    udpChannel, dispatcher, channelStatus, ctx);

                if (!udpChannel.isManualControlMode())
                {
                    localSocketAddressIndicator = ReceiveLocalSocketAddress.allocate(
                        tempBuffer, countersManager, registrationId, channelEndpoint.statusIndicatorCounter().id());

                    channelEndpoint.localSocketAddressIndicator(localSocketAddressIndicator);
                }

                validateInitialWindowForRcvBuf(params, udpChannel, channelEndpoint.socketRcvbufLength(), ctx);

                receiveChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
                receiverProxy.registerReceiveChannelEndpoint(channelEndpoint);
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(channelStatus, localSocketAddressIndicator, channelEndpoint);
                throw ex;
            }
        }
        else
        {
            validateInitialWindowForRcvBuf(params, udpChannel, channelEndpoint.socketRcvbufLength(), ctx);
            validateChannelBufferLength(
                SOCKET_RCVBUF_PARAM_NAME,
                udpChannel.socketRcvbufLength(),
                channelEndpoint.socketRcvbufLength(),
                udpChannel.originalUriString());
            validateChannelBufferLength(
                SOCKET_SNDBUF_PARAM_NAME,
                udpChannel.socketSndbufLength(),
                channelEndpoint.socketSndbufLength(),
                udpChannel.originalUriString());
        }

        return channelEndpoint;
    }

    private ReceiveChannelEndpoint findExistingReceiveChannelEndpoint(final UdpChannel udpChannel)
    {
        if (udpChannel.hasTag())
        {
            for (final ReceiveChannelEndpoint endpoint : receiveChannelEndpointByChannelMap.values())
            {
                if (endpoint.matchesTag(udpChannel))
                {
                    return endpoint;
                }
            }
        }

        ReceiveChannelEndpoint endpoint = receiveChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null != endpoint && endpoint.hasTag() && udpChannel.hasTag() && endpoint.tag() != udpChannel.tag())
        {
            endpoint = null;
        }

        return endpoint;
    }

    private AeronClient getOrAddClient(final long clientId)
    {
        AeronClient client = findClient(clients, clientId);
        if (null == client)
        {
            final AtomicCounter counter = ClientHeartbeatTimestamp.allocate(tempBuffer, countersManager, clientId);
            final int counterId = counter.id();

            counter.setOrdered(cachedEpochClock.time());
            countersManager.setCounterOwnerId(counterId, clientId);
            countersManager.setCounterRegistrationId(counterId, clientId);

            client = new AeronClient(
                clientId,
                clientLivenessTimeoutNs,
                ctx.systemCounters().get(SystemCounterDescriptor.CLIENT_TIMEOUTS),
                counter);
            clients.add(client);

            clientProxy.onCounterReady(clientId, counterId);
        }

        return client;
    }

    private IpcPublication getOrAddIpcPublication(
        final long correlationId,
        final long clientId,
        final int streamId,
        final String channel,
        final boolean isExclusive)
    {
        IpcPublication publication = null;
        final ChannelUri channelUri = ChannelUri.parse(channel);
        final PublicationParams params = getPublicationParams(channelUri, ctx, this, isExclusive, true);

        if (!isExclusive)
        {
            publication = findSharedIpcPublication(ipcPublications, streamId);
        }

        if (null == publication)
        {
            if (params.hasSessionId)
            {
                checkForSessionClash(params.sessionId, streamId, IPC_MEDIA);
            }

            validateMtuForMaxMessage(params);
            publication = addIpcPublication(correlationId, clientId, streamId, channel, isExclusive, params);
        }
        else
        {
            confirmMatch(channelUri, params, publication.rawLog(), publication.sessionId());
        }

        return publication;
    }

    private IpcPublication addIpcPublication(
        final long registrationId,
        final long clientId,
        final int streamId,
        final String channel,
        final boolean isExclusive,
        final PublicationParams params)
    {
        final int sessionId = params.hasSessionId ? params.sessionId : nextAvailableSessionId(streamId, IPC_MEDIA);
        final int initialTermId = params.hasPosition ? params.initialTermId : BitUtil.generateRandomisedId();
        final RawLog rawLog = newIpcPublicationLog(sessionId, streamId, initialTermId, registrationId, params);

        UnsafeBufferPosition publisherPosition = null;
        UnsafeBufferPosition publisherLimit = null;
        try
        {
            publisherPosition = PublisherPos.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);
            publisherLimit = PublisherLimit.allocate(
                tempBuffer, countersManager, registrationId, sessionId, streamId, channel);

            countersManager.setCounterOwnerId(publisherLimit.id(), clientId);

            if (params.hasPosition)
            {
                final int positionBitsToShift = positionBitsToShift(params.termLength);
                final long position = computePosition(
                    params.termId, params.termOffset, positionBitsToShift, initialTermId);
                publisherPosition.setOrdered(position);
                publisherLimit.setOrdered(position);
            }

            final IpcPublication publication = new IpcPublication(
                registrationId,
                channel,
                ctx,
                params.entityTag,
                sessionId,
                streamId,
                publisherPosition,
                publisherLimit,
                rawLog,
                Configuration.producerWindowLength(params.termLength, ctx.ipcPublicationTermWindowLength()),
                isExclusive);

            ipcPublications.add(publication);
            activeSessionSet.add(new SessionKey(sessionId, streamId, IPC_MEDIA));

            return publication;
        }
        catch (final Throwable ex)
        {
            CloseHelper.quietCloseAll(rawLog, publisherPosition, publisherLimit);
            throw ex;
        }
    }

    private static AeronClient findClient(final ArrayList<AeronClient> clients, final long clientId)
    {
        AeronClient aeronClient = null;

        for (int i = 0, size = clients.size(); i < size; i++)
        {
            final AeronClient client = clients.get(i);
            if (client.clientId() == clientId)
            {
                aeronClient = client;
                break;
            }
        }

        return aeronClient;
    }

    private static SubscriptionLink removeSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId)
            {
                subscriptionLink = subscription;
                fastUnorderedRemove(subscriptionLinks, i);
                break;
            }
        }

        return subscriptionLink;
    }

    private static IpcPublication findSharedIpcPublication(
        final ArrayList<IpcPublication> ipcPublications, final long streamId)
    {
        IpcPublication ipcPublication = null;

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.streamId() == streamId &&
                !publication.isExclusive() &&
                IpcPublication.State.ACTIVE == publication.state())
            {
                ipcPublication = publication;
                break;
            }
        }

        return ipcPublication;
    }

    private void checkForSessionClash(final int sessionId, final int streamId, final String channel)
    {
        if (activeSessionSet.contains(new SessionKey(sessionId, streamId, channel)))
        {
            throw new InvalidChannelException("existing publication has clashing sessionId=" + sessionId +
                " for streamId=" + streamId + " channel=" + channel);
        }
    }

    private int nextAvailableSessionId(final int streamId, final String channel)
    {
        final SessionKey sessionKey = new SessionKey(streamId, channel);

        while (true)
        {
            int sessionId = nextSessionId++;

            if (ctx.publicationReservedSessionIdLow() <= sessionId &&
                sessionId <= ctx.publicationReservedSessionIdHigh())
            {
                nextSessionId = ctx.publicationReservedSessionIdHigh() + 1;
                sessionId = nextSessionId++;
            }

            sessionKey.sessionId = sessionId;
            if (!activeSessionSet.contains(sessionKey))
            {
                return sessionId;
            }
        }
    }

    private <T extends DriverManagedResource> void checkManagedResources(
        final ArrayList<T> list, final long nowNs, final long nowMs)
    {
        for (int lastIndex = list.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final DriverManagedResource resource = list.get(i);

            resource.onTimeEvent(nowNs, nowMs, this);

            if (resource.hasReachedEndOfLife())
            {
                if (resource.free())
                {
                    fastUnorderedRemove(list, i, lastIndex--);
                    CloseHelper.close(ctx.errorHandler(), resource);
                }
                else
                {
                    ctx.systemCounters().get(FREE_FAILS).incrementOrdered();
                }
            }
        }
    }

    private void linkSpies(final ArrayList<SubscriptionLink> links, final NetworkPublication publication)
    {
        for (int i = 0, size = links.size(); i < size; i++)
        {
            final SubscriptionLink subscription = links.get(i);
            if (subscription.matches(publication) && !subscription.isLinked(publication))
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    publication.streamId(),
                    publication.sessionId(),
                    subscription.registrationId(),
                    linkSpy(publication, subscription).id(),
                    publication.rawLog().fileName(),
                    CommonContext.IPC_CHANNEL);
            }
        }
    }

    private void trackTime(final long nowNs)
    {
        final long cycleTimeNs = nowNs - cachedNanoClock.nanoTime();

        cachedNanoClock.update(nowNs);
        maxCycleTime.proposeMaxOrdered(cycleTimeNs);

        if (clockUpdateDeadlineNs - nowNs < 0)
        {
            clockUpdateDeadlineNs = nowNs + CLOCK_UPDATE_INTERNAL_NS;
            cachedEpochClock.update(epochClock.time());
        }

        if (cycleTimeNs > ctx.conductorCycleThresholdNs())
        {
            cycleTimeThresholdExceededCount.incrementOrdered();
        }
    }

    private int processTimers(final long nowNs)
    {
        int workCount = 0;

        if (timerCheckDeadlineNs - nowNs < 0)
        {
            timerCheckDeadlineNs = nowNs + timerIntervalNs;
            heartbeatAndCheckTimers(nowNs);
            checkForBlockedToDriverCommands(nowNs);
            workCount = 1;
        }

        return workCount;
    }

    private static boolean isOldestSubscriptionSparse(final ArrayList<SubscriberPosition> subscriberPositions)
    {
        final SubscriberPosition subscriberPosition = subscriberPositions.get(0);
        long regId = subscriberPosition.subscription().registrationId();
        boolean isSparse = subscriberPosition.subscription().isSparse();

        for (int i = 1, size = subscriberPositions.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriberPositions.get(i).subscription();
            if (subscription.registrationId() < regId)
            {
                isSparse = subscription.isSparse();
                regId = subscription.registrationId();
            }
        }

        return isSparse;
    }

    private int trackStreamPositions(final int existingWorkCount, final long nowNs)
    {
        int workCount = existingWorkCount;

        final ArrayList<PublicationImage> publicationImages = this.publicationImages;
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            workCount += publicationImages.get(i).trackRebuild(nowNs);
        }

        final ArrayList<NetworkPublication> networkPublications = this.networkPublications;
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            workCount += networkPublications.get(i).updatePublisherLimit();
        }

        final ArrayList<IpcPublication> ipcPublications = this.ipcPublications;
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            workCount += ipcPublications.get(i).updatePublisherLimit();
        }

        return workCount;
    }

    private static void validateChannelBufferLength(
        final String paramName, final int newLength, final int existingLength, final String channel)
    {
        if (0 != newLength && newLength != existingLength)
        {
            final String existingValue = 0 == existingLength ? "OS default" : Integer.toString(existingLength);

            throw new InvalidChannelException(
                paramName + "=" + newLength + " does not match existing value of " + existingValue + " uri=" + channel);
        }
    }

    private static void validateTimestampConfiguration(final UdpChannel udpChannel)
    {
        if (null != udpChannel.channelUri().get(PACKET_TIMESTAMP_OFFSET))
        {
            throw new InvalidChannelException(
                "Packet timestamps '" + PACKET_TIMESTAMP_OFFSET + "' are not supported in the Java driver");
        }
    }

    private static void validateDestinationUri(final ChannelUri uri, final String destinationUri)
    {
        if (SPY_QUALIFIER.equals(uri.prefix()))
        {
            throw new InvalidChannelException("Aeron spies are invalid as send destinations: " + destinationUri);
        }

        for (final String invalidKey : INVALID_DESTINATION_KEYS)
        {
            if (uri.containsKey(invalidKey))
            {
                throw new InvalidChannelException(
                    "destinations must not contain the key: " + invalidKey + " uri=" + destinationUri);
            }
        }
    }
}
