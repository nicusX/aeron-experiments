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

import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.LogBufferUnblocker;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static io.aeron.driver.Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS;
import static io.aeron.driver.Configuration.PUBLICATION_SETUP_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.logbuffer.TermScanner.*;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_AND_END_FLAGS;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_END_AND_EOS_FLAGS;
import static org.agrona.BitUtil.SIZE_OF_LONG;

class NetworkPublicationPadding1
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

class NetworkPublicationConductorFields extends NetworkPublicationPadding1
{
    static final ReadablePosition[] EMPTY_POSITIONS = new ReadablePosition[0];

    long cleanPosition;
    long timeOfLastActivityNs;
    long lastSenderPosition;
    int refCount = 0;
    ReadablePosition[] spyPositions = EMPTY_POSITIONS;
    final ArrayList<UntetheredSubscription> untetheredSubscriptions = new ArrayList<>();
}

class NetworkPublicationPadding2 extends NetworkPublicationConductorFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

class NetworkPublicationSenderFields extends NetworkPublicationPadding2
{
    long timeOfLastDataOrHeartbeatNs;
    long timeOfLastSetupNs;
    long timeOfLastStatusMessageNs;
    boolean trackSenderLimits = false;
    boolean isSetupElicited = false;
    boolean hasInitialConnection = false;
}

class NetworkPublicationPadding3 extends NetworkPublicationSenderFields
{
    byte p128, p129, p130, p131, p132, p133, p134, p135, p136, p137, p138, p139, p140, p142, p143, p144;
    byte p145, p146, p147, p148, p149, p150, p151, p152, p153, p154, p155, p156, p157, p158, p159, p160;
    byte p161, p162, p163, p164, p165, p166, p167, p168, p169, p170, p171, p172, p173, p174, p175, p176;
    byte p177, p178, p179, p180, p181, p182, p183, p184, p185, p186, p187, p189, p190, p191, p192, p193;
}

/**
 * Publication to be sent to connected subscribers.
 */
public final class NetworkPublication
    extends NetworkPublicationPadding3
    implements RetransmitSender, DriverManagedResource, Subscribable
{
    enum State
    {
        ACTIVE, DRAINING, LINGER, DONE
    }

    private final long registrationId;
    private final long unblockTimeoutNs;
    private final long connectionTimeoutNs;
    private final long lingerTimeoutNs;
    private final long untetheredWindowLimitTimeoutNs;
    private final long untetheredRestingTimeoutNs;
    private final long tag;
    private final int positionBitsToShift;
    private final int initialTermId;
    private final int termBufferLength;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;
    private final int sessionId;
    private final int streamId;
    private final boolean isExclusive;
    private final boolean spiesSimulateConnection;
    private final boolean signalEos;
    private volatile boolean hasReceivers;
    private volatile boolean hasSpies;
    private volatile boolean isConnected;
    private volatile boolean isEndOfStream;
    private volatile boolean hasSenderReleased;
    private State state = State.ACTIVE;

    private final UnsafeBuffer[] termBuffers;
    private final ByteBuffer[] sendBuffers;
    private final ErrorHandler errorHandler;
    private final Position publisherPos;
    private final Position publisherLimit;
    private final Position senderPosition;
    private final Position senderLimit;
    private final SendChannelEndpoint channelEndpoint;
    private final ByteBuffer heartbeatBuffer;
    private final DataHeaderFlyweight heartbeatDataHeader;
    private final ByteBuffer setupBuffer;
    private final SetupFlyweight setupHeader;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementHeader;
    private final FlowControl flowControl;
    private final CachedNanoClock cachedNanoClock;
    private final RetransmitHandler retransmitHandler;
    private final UnsafeBuffer metaDataBuffer;
    private final RawLog rawLog;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter senderFlowControlLimits;
    private final AtomicCounter senderBpe;
    private final AtomicCounter shortSends;
    private final AtomicCounter unblockedPublications;

    NetworkPublication(
        final long registrationId,
        final MediaDriver.Context ctx,
        final PublicationParams params,
        final SendChannelEndpoint channelEndpoint,
        final RawLog rawLog,
        final int termWindowLength,
        final Position publisherPos,
        final Position publisherLimit,
        final Position senderPosition,
        final Position senderLimit,
        final AtomicCounter senderBpe,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final FlowControl flowControl,
        final RetransmitHandler retransmitHandler,
        final NetworkPublicationThreadLocals threadLocals,
        final boolean isExclusive)
    {
        this.registrationId = registrationId;
        this.unblockTimeoutNs = ctx.publicationUnblockTimeoutNs();
        this.connectionTimeoutNs = ctx.publicationConnectionTimeoutNs();
        this.lingerTimeoutNs = params.lingerTimeoutNs;
        this.untetheredWindowLimitTimeoutNs = ctx.untetheredWindowLimitTimeoutNs();
        this.untetheredRestingTimeoutNs = ctx.untetheredRestingTimeoutNs();
        this.tag = params.entityTag;
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.cachedNanoClock = ctx.senderCachedNanoClock();
        this.senderPosition = senderPosition;
        this.senderLimit = senderLimit;
        this.flowControl = flowControl;
        this.retransmitHandler = retransmitHandler;
        this.publisherPos = publisherPos;
        this.publisherLimit = publisherLimit;
        this.mtuLength = params.mtuLength;
        this.initialTermId = initialTermId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.spiesSimulateConnection = params.spiesSimulateConnection;
        this.signalEos = params.signalEos;
        this.isExclusive = isExclusive;

        metaDataBuffer = rawLog.metaData();
        setupBuffer = threadLocals.setupBuffer();
        setupHeader = threadLocals.setupHeader();
        heartbeatBuffer = threadLocals.heartbeatBuffer();
        heartbeatDataHeader = threadLocals.heartbeatDataHeader();
        rttMeasurementBuffer = threadLocals.rttMeasurementBuffer();
        rttMeasurementHeader = threadLocals.rttMeasurementHeader();

        final SystemCounters systemCounters = ctx.systemCounters();
        heartbeatsSent = systemCounters.get(HEARTBEATS_SENT);
        shortSends = systemCounters.get(SHORT_SENDS);
        retransmitsSent = systemCounters.get(RETRANSMITS_SENT);
        senderFlowControlLimits = systemCounters.get(SENDER_FLOW_CONTROL_LIMITS);
        unblockedPublications = systemCounters.get(UNBLOCKED_PUBLICATIONS);
        this.senderBpe = senderBpe;

        termBuffers = rawLog.termBuffers();
        sendBuffers = rawLog.sliceTerms();
        errorHandler = ctx.errorHandler();

        final int termLength = rawLog.termLength();
        termBufferLength = termLength;
        termLengthMask = termLength - 1;

        final long nowNs = cachedNanoClock.nanoTime();
        timeOfLastDataOrHeartbeatNs = nowNs - PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        timeOfLastSetupNs = nowNs - PUBLICATION_SETUP_TIMEOUT_NS - 1;
        timeOfLastStatusMessageNs = nowNs;

        positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        this.termWindowLength = termWindowLength;

        lastSenderPosition = senderPosition.get();
        cleanPosition = lastSenderPosition;
        timeOfLastActivityNs = nowNs;
    }

    /**
     * {@inheritDoc}
     */
    public boolean free()
    {
        return rawLog.free();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(errorHandler, publisherPos);
        CloseHelper.close(errorHandler, publisherLimit);
        CloseHelper.close(errorHandler, senderPosition);
        CloseHelper.close(errorHandler, senderLimit);
        CloseHelper.close(errorHandler, senderBpe);
        CloseHelper.closeAll(errorHandler, spyPositions);

        for (int i = 0, size = untetheredSubscriptions.size(); i < size; i++)
        {
            final UntetheredSubscription untetheredSubscription = untetheredSubscriptions.get(i);
            if (UntetheredSubscription.State.RESTING == untetheredSubscription.state)
            {
                CloseHelper.close(errorHandler, untetheredSubscription.position);
            }
        }

        CloseHelper.close(errorHandler, rawLog);
    }

    /**
     * Time of the last status message a from a receiver.
     *
     * @return this of the last status message a from a receiver.
     */
    public long timeOfLastStatusMessageNs()
    {
        return timeOfLastStatusMessageNs;
    }

    /**
     * Channel URI string for this publication.
     *
     * @return channel URI string for this publication.
     */
    public String channel()
    {
        return channelEndpoint.originalUriString();
    }

    /**
     * Session id allocated to this stream.
     *
     * @return session id allocated to this stream.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * Stream id within the channel.
     *
     * @return stream id within the channel.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Trigger the sending of a SETUP frame so a connection can be established.
     */
    public void triggerSendSetupFrame()
    {
        if (!isEndOfStream)
        {
            timeOfLastStatusMessageNs = cachedNanoClock.nanoTime();
            isSetupElicited = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void addSubscriber(
        final SubscriptionLink subscriptionLink, final ReadablePosition position, final long nowNs)
    {
        spyPositions = ArrayUtil.add(spyPositions, position);
        hasSpies = true;

        if (!subscriptionLink.isTether())
        {
            untetheredSubscriptions.add(new UntetheredSubscription(subscriptionLink, position, nowNs));
        }

        if (spiesSimulateConnection)
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, true);
            isConnected = true;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeSubscriber(final SubscriptionLink subscriptionLink, final ReadablePosition position)
    {
        spyPositions = ArrayUtil.remove(spyPositions, position);
        hasSpies = spyPositions.length > 0;
        position.close();

        if (!subscriptionLink.isTether())
        {
            for (int lastIndex = untetheredSubscriptions.size() - 1, i = lastIndex; i >= 0; i--)
            {
                if (untetheredSubscriptions.get(i).subscriptionLink == subscriptionLink)
                {
                    ArrayListUtil.fastUnorderedRemove(untetheredSubscriptions, i, lastIndex);
                    break;
                }
            }
        }
    }

    /**
     * Process a NAK message so a retransmit can occur.
     *
     * @param termId     in which the loss occurred.
     * @param termOffset at which the loss begins.
     * @param length     of the loss.
     */
    public void onNak(final int termId, final int termOffset, final int length)
    {
        retransmitHandler.onNak(termId, termOffset, length, termBufferLength, this);
    }

    /**
     * Process a status message to track connectivity and apply flow control.
     *
     * @param msg        flyweight over the network packet.
     * @param srcAddress that the setup message has come from.
     */
    public void onStatusMessage(final StatusMessageFlyweight msg, final InetSocketAddress srcAddress)
    {
        if (!hasReceivers)
        {
            hasReceivers = true;
        }

        if (!hasInitialConnection)
        {
            hasInitialConnection = true;
        }

        final long timeNs = cachedNanoClock.nanoTime();
        timeOfLastStatusMessageNs = timeNs;

        senderLimit.setOrdered(flowControl.onStatusMessage(
            msg,
            srcAddress,
            senderLimit.get(),
            initialTermId,
            positionBitsToShift,
            timeNs));

        if (!isConnected && flowControl.hasRequiredReceivers())
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, true);
            isConnected = true;
        }
    }

    /**
     * Process a RTT (Round Trip Timing) message from a receiver.
     *
     * @param msg        flyweight over the network packet.
     * @param srcAddress that the RTT message has come from.
     */
    public void onRttMeasurement(final RttMeasurementFlyweight msg, final InetSocketAddress srcAddress)
    {
        if (RttMeasurementFlyweight.REPLY_FLAG == (msg.flags() & RttMeasurementFlyweight.REPLY_FLAG))
        {
            rttMeasurementBuffer.clear();
            rttMeasurementHeader
                .receiverId(msg.receiverId())
                .echoTimestampNs(msg.echoTimestampNs())
                .receptionDelta(0)
                .sessionId(sessionId)
                .streamId(streamId)
                .flags((short)0x0);

            final int bytesSent = channelEndpoint.send(rttMeasurementBuffer);
            if (RttMeasurementFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }
        }

        // handling of RTT measurements would be done in an else clause here.
    }

    /**
     * {@inheritDoc}
     */
    public void resend(final int termId, final int termOffset, final int length)
    {
        final long senderPosition = this.senderPosition.get();
        final long resendPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        final long bottomResendWindow = senderPosition - (termBufferLength >> 1);

        if (bottomResendWindow <= resendPosition && resendPosition < senderPosition)
        {
            final int activeIndex = indexByPosition(resendPosition, positionBitsToShift);
            final UnsafeBuffer termBuffer = termBuffers[activeIndex];
            final ByteBuffer sendBuffer = sendBuffers[activeIndex];

            int remainingBytes = length;
            int bytesSent = 0;
            int offset = termOffset;
            do
            {
                offset += bytesSent;

                final long scanOutcome = scanForAvailability(termBuffer, offset, Math.min(mtuLength, remainingBytes));
                final int available = available(scanOutcome);
                if (available <= 0)
                {
                    break;
                }

                sendBuffer.limit(offset + available).position(offset);

                if (available != channelEndpoint.send(sendBuffer))
                {
                    shortSends.increment();
                    break;
                }

                bytesSent = available + padding(scanOutcome);
                remainingBytes -= bytesSent;
            }
            while (remainingBytes > 0);

            retransmitsSent.incrementOrdered();
        }
    }

    int send(final long nowNs)
    {
        final long senderPosition = this.senderPosition.get();
        final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);
        final int termOffset = (int)senderPosition & termLengthMask;

        if (!hasInitialConnection || isSetupElicited)
        {
            setupMessageCheck(nowNs, activeTermId, termOffset);
        }

        int bytesSent = sendData(nowNs, senderPosition, termOffset);

        if (0 == bytesSent)
        {
            bytesSent = heartbeatMessageCheck(nowNs, activeTermId, termOffset, signalEos && isEndOfStream);

            if (spiesSimulateConnection && hasSpies && !hasReceivers)
            {
                final long newSenderPosition = maxSpyPosition(senderPosition);
                this.senderPosition.setOrdered(newSenderPosition);
                senderLimit.setOrdered(flowControl.onIdle(nowNs, newSenderPosition, newSenderPosition, isEndOfStream));
            }
            else
            {
                senderLimit.setOrdered(flowControl.onIdle(nowNs, senderLimit.get(), senderPosition, isEndOfStream));
            }
        }

        updateHasReceivers(nowNs);
        retransmitHandler.processTimeouts(nowNs, this);

        return bytesSent;
    }

    SendChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    RawLog rawLog()
    {
        return rawLog;
    }

    int publisherLimitId()
    {
        return publisherLimit.id();
    }

    long tag()
    {
        return tag;
    }

    int termBufferLength()
    {
        return termBufferLength;
    }

    int mtuLength()
    {
        return mtuLength;
    }

    long registrationId()
    {
        return registrationId;
    }

    boolean isExclusive()
    {
        return isExclusive;
    }

    boolean spiesSimulateConnection()
    {
        return spiesSimulateConnection;
    }

    boolean isAcceptingSubscriptions()
    {
        return State.ACTIVE == state ||
            (State.DRAINING == state && spyPositions.length > 0 && producerPosition() > senderPosition.getVolatile());
    }

    /**
     * Update the publishers limit for flow control as part of the conductor duty cycle.
     *
     * @return 1 if the limit has been updated otherwise 0.
     */
    int updatePublisherLimit()
    {
        int workCount = 0;

        if (State.ACTIVE == state)
        {
            final long senderPosition = this.senderPosition.getVolatile();
            if (hasRequiredReceivers() || (spiesSimulateConnection && spyPositions.length > 0))
            {
                long minConsumerPosition = senderPosition;
                for (final ReadablePosition spyPosition : spyPositions)
                {
                    minConsumerPosition = Math.min(minConsumerPosition, spyPosition.getVolatile());
                }

                final long proposedPublisherLimit = minConsumerPosition + termWindowLength;
                final long publisherLimit = this.publisherLimit.get();
                if (proposedPublisherLimit > publisherLimit)
                {
                    cleanBufferTo(minConsumerPosition - termBufferLength);
                    this.publisherLimit.setOrdered(proposedPublisherLimit);
                    workCount = 1;
                }
            }
            else if (publisherLimit.get() > senderPosition)
            {
                publisherLimit.setOrdered(senderPosition);
                cleanBufferTo(senderPosition - termBufferLength);
                workCount = 1;
            }
        }

        return workCount;
    }

    boolean hasSpies()
    {
        return hasSpies;
    }

    void updateHasReceivers(final long timeNs)
    {
        if (((timeOfLastStatusMessageNs + connectionTimeoutNs) - timeNs < 0) && hasReceivers)
        {
            hasReceivers = false;
        }
    }

    private int sendData(final long nowNs, final long senderPosition, final int termOffset)
    {
        int bytesSent = 0;
        final int availableWindow = (int)(senderLimit.get() - senderPosition);
        if (availableWindow > 0)
        {
            final int scanLimit = Math.min(availableWindow, mtuLength);
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);

            final long scanOutcome = scanForAvailability(termBuffers[activeIndex], termOffset, scanLimit);
            final int available = available(scanOutcome);
            if (available > 0)
            {
                final ByteBuffer sendBuffer = sendBuffers[activeIndex];
                sendBuffer.limit(termOffset + available).position(termOffset);

                if (available == channelEndpoint.send(sendBuffer))
                {
                    timeOfLastDataOrHeartbeatNs = nowNs;
                    trackSenderLimits = true;

                    bytesSent = available;
                    this.senderPosition.setOrdered(senderPosition + bytesSent + padding(scanOutcome));
                }
                else
                {
                    shortSends.increment();
                }
            }
        }
        else if (trackSenderLimits)
        {
            trackSenderLimits = false;
            senderBpe.incrementOrdered();
            senderFlowControlLimits.incrementOrdered();
        }

        return bytesSent;
    }

    private void setupMessageCheck(final long nowNs, final int activeTermId, final int termOffset)
    {
        if ((timeOfLastSetupNs + PUBLICATION_SETUP_TIMEOUT_NS) - nowNs < 0)
        {
            timeOfLastSetupNs = nowNs;

            setupBuffer.clear();
            setupHeader
                .activeTermId(activeTermId)
                .termOffset(termOffset)
                .sessionId(sessionId)
                .streamId(streamId)
                .initialTermId(initialTermId)
                .termLength(termBufferLength)
                .mtuLength(mtuLength)
                .ttl(channelEndpoint.multicastTtl());

            if (SetupFlyweight.HEADER_LENGTH != channelEndpoint.send(setupBuffer))
            {
                shortSends.increment();
            }

            if (isSetupElicited && hasReceivers)
            {
                isSetupElicited = false;
            }
        }
    }

    private int heartbeatMessageCheck(
        final long nowNs, final int activeTermId, final int termOffset, final boolean signalEos)
    {
        int bytesSent = 0;

        if (hasInitialConnection && (timeOfLastDataOrHeartbeatNs + PUBLICATION_HEARTBEAT_TIMEOUT_NS) - nowNs < 0)
        {
            heartbeatBuffer.clear();
            heartbeatDataHeader
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(activeTermId)
                .termOffset(termOffset)
                .flags((byte)(signalEos ? BEGIN_END_AND_EOS_FLAGS : BEGIN_AND_END_FLAGS));

            bytesSent = channelEndpoint.send(heartbeatBuffer);
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }

            timeOfLastDataOrHeartbeatNs = nowNs;
            heartbeatsSent.incrementOrdered();
        }

        return bytesSent;
    }

    private void cleanBufferTo(final long position)
    {
        final long cleanPosition = this.cleanPosition;
        if (position > cleanPosition)
        {
            final UnsafeBuffer dirtyTerm = termBuffers[indexByPosition(cleanPosition, positionBitsToShift)];
            final int bytesForCleaning = (int)(position - cleanPosition);
            final int termOffset = (int)cleanPosition & termLengthMask;
            final int length = Math.min(bytesForCleaning, termBufferLength - termOffset);

            dirtyTerm.setMemory(termOffset + SIZE_OF_LONG, length - SIZE_OF_LONG, (byte)0);
            dirtyTerm.putLongOrdered(termOffset, 0);
            this.cleanPosition = cleanPosition + length;
        }
    }

    private void checkForBlockedPublisher(final long producerPosition, final long senderPosition, final long nowNs)
    {
        if (senderPosition == lastSenderPosition && isPossiblyBlocked(producerPosition, senderPosition))
        {
            if ((timeOfLastActivityNs + unblockTimeoutNs) - nowNs < 0)
            {
                if (LogBufferUnblocker.unblock(termBuffers, metaDataBuffer, senderPosition, termBufferLength))
                {
                    unblockedPublications.incrementOrdered();
                }
            }
        }
        else
        {
            timeOfLastActivityNs = nowNs;
            lastSenderPosition = senderPosition;
        }
    }

    private boolean isPossiblyBlocked(final long producerPosition, final long consumerPosition)
    {
        final int producerTermCount = activeTermCount(metaDataBuffer);
        final int expectedTermCount = (int)(consumerPosition >> positionBitsToShift);

        if (producerTermCount != expectedTermCount)
        {
            return true;
        }

        return producerPosition > consumerPosition;
    }

    private boolean spiesFinishedConsuming(final DriverConductor conductor, final long eosPosition)
    {
        if (spyPositions.length > 0)
        {
            for (final ReadablePosition spyPosition : spyPositions)
            {
                if (spyPosition.getVolatile() < eosPosition)
                {
                    return false;
                }
            }

            hasSpies = false;
            conductor.cleanupSpies(this);
        }

        return true;
    }

    private long maxSpyPosition(final long senderPosition)
    {
        long position = senderPosition;

        for (final ReadablePosition spyPosition : spyPositions)
        {
            position = Math.max(position, spyPosition.getVolatile());
        }

        return position;
    }

    private void updateConnectedStatus()
    {
        final boolean currentConnectedState =
            hasRequiredReceivers() || (spiesSimulateConnection && spyPositions.length > 0);

        if (currentConnectedState != isConnected)
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, currentConnectedState);
            isConnected = currentConnectedState;
        }
    }

    private boolean hasRequiredReceivers()
    {
        return hasReceivers && flowControl.hasRequiredReceivers();
    }

    private void checkUntetheredSubscriptions(final long nowNs, final DriverConductor conductor)
    {
        final ArrayList<UntetheredSubscription> untetheredSubscriptions = this.untetheredSubscriptions;
        final int untetheredSubscriptionsSize = untetheredSubscriptions.size();
        if (untetheredSubscriptionsSize > 0)
        {
            final long senderPosition = this.senderPosition.getVolatile();
            final long untetheredWindowLimit = (senderPosition - termWindowLength) + (termWindowLength >> 2);

            for (int lastIndex = untetheredSubscriptionsSize - 1, i = lastIndex; i >= 0; i--)
            {
                final UntetheredSubscription untethered = untetheredSubscriptions.get(i);
                if (UntetheredSubscription.State.ACTIVE == untethered.state)
                {
                    if (untethered.position.getVolatile() > untetheredWindowLimit)
                    {
                        untethered.timeOfLastUpdateNs = nowNs;
                    }
                    else if ((untethered.timeOfLastUpdateNs + untetheredWindowLimitTimeoutNs) - nowNs <= 0)
                    {
                        conductor.notifyUnavailableImageLink(registrationId, untethered.subscriptionLink);
                        untethered.state(UntetheredSubscription.State.LINGER, nowNs, streamId, sessionId);
                    }
                }
                else if (UntetheredSubscription.State.LINGER == untethered.state)
                {
                    if ((untethered.timeOfLastUpdateNs + untetheredWindowLimitTimeoutNs) - nowNs <= 0)
                    {
                        spyPositions = ArrayUtil.remove(spyPositions, untethered.position);
                        untethered.state(UntetheredSubscription.State.RESTING, nowNs, streamId, sessionId);
                    }
                }
                else if (UntetheredSubscription.State.RESTING == untethered.state)
                {
                    if ((untethered.timeOfLastUpdateNs + untetheredRestingTimeoutNs) - nowNs <= 0)
                    {
                        spyPositions = ArrayUtil.add(spyPositions, untethered.position);
                        conductor.notifyAvailableImageLink(
                            registrationId,
                            sessionId,
                            untethered.subscriptionLink,
                            untethered.position.id(),
                            senderPosition,
                            rawLog.fileName(),
                            CommonContext.IPC_CHANNEL);
                        untethered.state(UntetheredSubscription.State.ACTIVE, nowNs, streamId, sessionId);
                        LogBufferDescriptor.isConnected(metaDataBuffer, true);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        switch (state)
        {
            case ACTIVE:
            {
                updateConnectedStatus();
                final long producerPosition = producerPosition();
                publisherPos.setOrdered(producerPosition);
                if (!isExclusive)
                {
                    checkForBlockedPublisher(producerPosition, senderPosition.getVolatile(), timeNs);
                }
                checkUntetheredSubscriptions(timeNs, conductor);
                break;
            }

            case DRAINING:
            {
                final long producerPosition = producerPosition();
                publisherPos.setOrdered(producerPosition);
                final long senderPosition = this.senderPosition.getVolatile();
                if (producerPosition > senderPosition)
                {
                    if (LogBufferUnblocker.unblock(termBuffers, metaDataBuffer, senderPosition, termBufferLength))
                    {
                        unblockedPublications.incrementOrdered();
                        break;
                    }

                    if (hasReceivers)
                    {
                        break;
                    }
                }
                else
                {
                    isEndOfStream = true;
                }

                if (spiesFinishedConsuming(conductor, producerPosition))
                {
                    timeOfLastActivityNs = timeNs;
                    state = State.LINGER;
                }
                break;
            }

            case LINGER:
                if ((timeOfLastActivityNs + lingerTimeoutNs) - timeNs < 0)
                {
                    channelEndpoint.decRef();
                    conductor.cleanupPublication(this);
                    timeOfLastActivityNs = timeNs;
                    state = State.DONE;
                }
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasReachedEndOfLife()
    {
        return hasSenderReleased;
    }

    void decRef()
    {
        if (0 == --refCount)
        {
            final long producerPosition = producerPosition();
            publisherLimit.setOrdered(producerPosition);
            endOfStreamPosition(metaDataBuffer, producerPosition);

            if (senderPosition.getVolatile() >= producerPosition)
            {
                isEndOfStream = true;
            }

            state = State.DRAINING;
        }
    }

    void incRef()
    {
        ++refCount;
    }

    State state()
    {
        return state;
    }

    void senderRelease()
    {
        hasSenderReleased = true;
    }

    long producerPosition()
    {
        final long rawTail = rawTailVolatile(metaDataBuffer);
        final int termOffset = termOffset(rawTail, termBufferLength);

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    long consumerPosition()
    {
        return senderPosition.getVolatile();
    }
}
