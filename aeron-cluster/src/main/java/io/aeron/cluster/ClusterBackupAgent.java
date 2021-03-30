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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.*;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.BackupResponseDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.*;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.TAGS_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.*;
import static io.aeron.cluster.ClusterBackup.State.*;
import static io.aeron.exceptions.AeronException.Category;
import static io.aeron.exceptions.AeronException.Category.WARN;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

/**
 * {@link Agent} which backs up a remote cluster by replicating the log and polling for snapshots.
 */
public final class ClusterBackupAgent implements Agent
{
    /**
     * Update interval for cluster mark file.
     */
    public static final long MARK_FILE_UPDATE_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

    private static final int SLOW_TICK_INTERVAL_MS = 10;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final BackupResponseDecoder backupResponseDecoder = new BackupResponseDecoder();

    private final ClusterBackup.Context ctx;
    private final ClusterMarkFile markFile;
    private final AgentInvoker aeronClientInvoker;
    private final EpochClock epochClock;
    private final Aeron aeron;
    private final String[] clusterConsensusEndpoints;
    private final ConsensusPublisher consensusPublisher = new ConsensusPublisher();
    private final ArrayList<RecordingLog.Snapshot> snapshotsToRetrieve = new ArrayList<>(4);
    private final ArrayList<RecordingLog.Snapshot> snapshotsRetrieved = new ArrayList<>(4);
    private final Counter stateCounter;
    private final Counter liveLogPositionCounter;
    private final Counter nextQueryDeadlineMsCounter;
    private final ClusterBackupEventsListener eventsListener;
    private final long backupResponseTimeoutMs;
    private final long backupQueryIntervalMs;
    private final long backupProgressTimeoutMs;
    private final long coolDownIntervalMs;
    private final long unavailableCounterHandlerRegistrationId;

    private ClusterBackup.State state = BACKUP_QUERY;

    private RecordingSignalPoller recordingSignalPoller;
    private AeronArchive backupArchive;
    private AeronArchive.AsyncConnect clusterArchiveAsyncConnect;
    private AeronArchive clusterArchive;

    private SnapshotReplication snapshotReplication;

    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);
    private final Subscription consensusSubscription;
    private ExclusivePublication consensusPublication;
    private ClusterMember[] clusterMembers;
    private ClusterMember leaderMember;
    private RecordingLog recordingLog;
    private RecordingLog.Entry leaderLogEntry;
    private RecordingLog.Entry leaderLastTermEntry;
    private Subscription recordingSubscription = null;
    private String replayChannel = null;
    private String recordingChannel = null;

    private long slowTickDeadlineMs = 0;
    private long markFileUpdateDeadlineMs = 0;
    private long timeOfLastBackupQueryMs = 0;
    private long timeOfLastProgressMs = 0;
    private long coolDownDeadlineMs = NULL_VALUE;
    private long correlationId = NULL_VALUE;
    private long leaderLogRecordingId = NULL_VALUE;
    private long liveLogRecordingSubscriptionId = NULL_VALUE;
    private long liveLogRecordingId = NULL_VALUE;
    private int leaderCommitPositionCounterId = NULL_VALUE;
    private int clusterConsensusEndpointsCursor = NULL_VALUE;
    private int snapshotCursor = 0;
    private int liveLogReplaySessionId = NULL_VALUE;
    private int liveLogRecCounterId = NULL_COUNTER_ID;

    ClusterBackupAgent(final ClusterBackup.Context ctx)
    {
        this.ctx = ctx;
        aeron = ctx.aeron();
        epochClock = ctx.epochClock();

        backupResponseTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupResponseTimeoutNs());
        backupQueryIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupIntervalNs());
        backupProgressTimeoutMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupProgressTimeoutNs());
        coolDownIntervalMs = TimeUnit.NANOSECONDS.toMillis(ctx.clusterBackupCoolDownIntervalNs());
        markFile = ctx.clusterMarkFile();
        eventsListener = ctx.eventsListener();

        clusterConsensusEndpoints = ctx.clusterConsensusEndpoints().split(",");

        aeronClientInvoker = aeron.conductorAgentInvoker();
        aeronClientInvoker.invoke();

        unavailableCounterHandlerRegistrationId = aeron.addUnavailableCounterHandler(this::onUnavailableCounter);

        consensusSubscription = aeron.addSubscription(ctx.consensusChannel(), ctx.consensusStreamId());

        stateCounter = ctx.stateCounter();
        liveLogPositionCounter = ctx.liveLogPositionCounter();
        nextQueryDeadlineMsCounter = ctx.nextQueryDeadlineMsCounter();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        recordingLog = new RecordingLog(ctx.clusterDir());
        backupArchive = AeronArchive.connect(ctx.archiveContext().clone());
        recordingSignalPoller = new RecordingSignalPoller(
            backupArchive.controlSessionId(), backupArchive.controlResponsePoller().subscription());
        nextQueryDeadlineMsCounter.setOrdered(epochClock.time() - 1);
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        if (!aeron.isClosed())
        {
            aeron.removeUnavailableCounterHandler(unavailableCounterHandlerRegistrationId);

            if (NULL_VALUE != liveLogRecordingSubscriptionId)
            {
                backupArchive.tryStopRecording(liveLogRecordingSubscriptionId);
            }

            if (null != snapshotReplication)
            {
                snapshotReplication.close(backupArchive);
            }

            if (!ctx.ownsAeronClient())
            {
                CloseHelper.closeAll(consensusSubscription, consensusPublication, recordingSubscription);
            }

            state(CLOSED, epochClock.time());
        }

        CloseHelper.closeAll(backupArchive, clusterArchiveAsyncConnect, clusterArchive, recordingLog);
        markFile.updateActivityTimestamp(NULL_VALUE);
        ctx.close();
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        final long nowMs = epochClock.time();
        int workCount = 0;

        if (nowMs > slowTickDeadlineMs)
        {
            workCount += slowTick(nowMs);
            slowTickDeadlineMs = nowMs + SLOW_TICK_INTERVAL_MS;
        }

        try
        {
            workCount += consensusSubscription.poll(fragmentAssembler, ConsensusAdapter.FRAGMENT_LIMIT);

            switch (state)
            {
                case BACKUP_QUERY:
                    workCount += backupQuery(nowMs);
                    break;

                case SNAPSHOT_RETRIEVE:
                    workCount += snapshotRetrieve(nowMs);
                    break;

                case LIVE_LOG_RECORD:
                    workCount += liveLogRecord(nowMs);
                    break;

                case LIVE_LOG_REPLAY:
                    workCount += liveLogReplay(nowMs);
                    break;

                case UPDATE_RECORDING_LOG:
                    workCount += updateRecordingLog(nowMs);
                    break;

                case BACKING_UP:
                    workCount += backingUp(nowMs);
                    break;

                case RESET_BACKUP:
                    workCount += resetBackup(nowMs);
                    break;
            }

            if (hasProgressStalled(nowMs))
            {
                if (null != eventsListener)
                {
                    eventsListener.onPossibleFailure(new TimeoutException("progress has stalled", Category.WARN));
                }

                state(RESET_BACKUP, nowMs);
            }
        }
        catch (final AgentTerminationException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            if (null != eventsListener)
            {
                eventsListener.onPossibleFailure(ex);
            }

            state(RESET_BACKUP, nowMs);
            throw ex;
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "cluster-backup";
    }

    private void reset()
    {
        clusterMembers = null;
        leaderMember = null;
        leaderLogEntry = null;
        leaderLastTermEntry = null;
        clusterConsensusEndpointsCursor = NULL_VALUE;
        liveLogRecCounterId = NULL_COUNTER_ID;
        liveLogRecordingId = NULL_VALUE;

        snapshotsToRetrieve.clear();
        snapshotsRetrieved.clear();
        fragmentAssembler.clear();

        if (null != snapshotReplication)
        {
            snapshotReplication.close(backupArchive);
            snapshotReplication = null;
        }

        if (NULL_VALUE != liveLogRecordingSubscriptionId)
        {
            try
            {
                backupArchive.tryStopRecording(liveLogRecordingSubscriptionId);
            }
            catch (final Throwable ex)
            {
                ctx.countedErrorHandler().onError(ex);
            }
            liveLogRecordingSubscriptionId = NULL_VALUE;
        }

        CloseHelper.closeAll(
            (ErrorHandler)ctx.countedErrorHandler(),
            consensusPublication,
            clusterArchive,
            clusterArchiveAsyncConnect,
            recordingSubscription);

        consensusPublication = null;
        clusterArchive = null;
        clusterArchiveAsyncConnect = null;
        recordingSubscription = null;
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        if (messageHeaderDecoder.templateId() == BackupResponseDecoder.TEMPLATE_ID)
        {
            backupResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            onBackupResponse(
                backupResponseDecoder.correlationId(),
                backupResponseDecoder.logRecordingId(),
                backupResponseDecoder.logLeadershipTermId(),
                backupResponseDecoder.logTermBaseLogPosition(),
                backupResponseDecoder.lastLeadershipTermId(),
                backupResponseDecoder.lastTermBaseLogPosition(),
                backupResponseDecoder.commitPositionCounterId(),
                backupResponseDecoder.leaderMemberId(),
                backupResponseDecoder);
        }
    }

    private void onUnavailableCounter(final CountersReader counters, final long registrationId, final int counterId)
    {
        if (counterId == liveLogRecCounterId)
        {
            if (null != eventsListener)
            {
                eventsListener.onPossibleFailure(new ClusterException(
                    "log recording counter unexpectedly unavailable", Category.WARN));
            }

            state(RESET_BACKUP, epochClock.time());
        }
    }

    @SuppressWarnings("MethodLength")
    private void onBackupResponse(
        final long correlationId,
        final long logRecordingId,
        final long logLeadershipTermId,
        final long logTermBaseLogPosition,
        final long lastLeadershipTermId,
        final long lastTermBaseLogPosition,
        final int commitPositionCounterId,
        final int leaderMemberId,
        final BackupResponseDecoder backupResponseDecoder)
    {
        if (BACKUP_QUERY == state && correlationId == this.correlationId)
        {
            final BackupResponseDecoder.SnapshotsDecoder snapshotsDecoder = backupResponseDecoder.snapshots();

            if (snapshotsDecoder.count() > 0)
            {
                for (final BackupResponseDecoder.SnapshotsDecoder snapshot : snapshotsDecoder)
                {
                    final RecordingLog.Entry entry = recordingLog.getLatestSnapshot(snapshot.serviceId());

                    if (null != entry && snapshot.logPosition() == entry.logPosition)
                    {
                        continue;
                    }

                    snapshotsToRetrieve.add(new RecordingLog.Snapshot(
                        snapshot.recordingId(),
                        snapshot.leadershipTermId(),
                        snapshot.termBaseLogPosition(),
                        snapshot.logPosition(),
                        snapshot.timestamp(),
                        snapshot.serviceId()));
                }
            }

            if (null == leaderMember || leaderMember.id() != leaderMemberId || logRecordingId != leaderLogRecordingId)
            {
                leaderLogRecordingId = logRecordingId;
                leaderLogEntry = new RecordingLog.Entry(
                    logRecordingId,
                    logLeadershipTermId,
                    logTermBaseLogPosition,
                    NULL_POSITION,
                    NULL_TIMESTAMP,
                    NULL_VALUE,
                    RecordingLog.ENTRY_TYPE_TERM,
                    true,
                    -1);
            }

            final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();

            if (null == lastTerm ||
                lastLeadershipTermId != lastTerm.leadershipTermId ||
                lastTermBaseLogPosition != lastTerm.termBaseLogPosition)
            {
                leaderLastTermEntry = new RecordingLog.Entry(
                    logRecordingId,
                    lastLeadershipTermId,
                    lastTermBaseLogPosition,
                    NULL_POSITION,
                    NULL_TIMESTAMP,
                    NULL_VALUE,
                    RecordingLog.ENTRY_TYPE_TERM,
                    true,
                    -1);
            }

            timeOfLastBackupQueryMs = 0;
            snapshotCursor = 0;
            this.correlationId = NULL_VALUE;
            leaderCommitPositionCounterId = commitPositionCounterId;

            clusterMembers = ClusterMember.parse(backupResponseDecoder.clusterMembers());
            leaderMember = ClusterMember.findMember(clusterMembers, leaderMemberId);

            if (null != eventsListener)
            {
                eventsListener.onBackupResponse(clusterMembers, leaderMember, snapshotsToRetrieve);
            }

            if (null == clusterArchive)
            {
                CloseHelper.close(clusterArchiveAsyncConnect);

                final ChannelUri leaderArchiveUri = ChannelUri.parse(ctx.archiveContext().controlRequestChannel());
                leaderArchiveUri.put(ENDPOINT_PARAM_NAME, leaderMember.archiveEndpoint());

                final AeronArchive.Context leaderArchiveCtx = new AeronArchive.Context()
                    .aeron(ctx.aeron())
                    .controlRequestChannel(leaderArchiveUri.toString())
                    .controlRequestStreamId(ctx.archiveContext().controlRequestStreamId())
                    .controlResponseChannel(ctx.archiveContext().controlResponseChannel())
                    .controlResponseStreamId(ctx.archiveContext().controlResponseStreamId());

                clusterArchiveAsyncConnect = AeronArchive.asyncConnect(leaderArchiveCtx);
            }

            final long nowMs = epochClock.time();
            timeOfLastProgressMs = nowMs;
            state(snapshotsToRetrieve.isEmpty() ? LIVE_LOG_RECORD : SNAPSHOT_RETRIEVE, nowMs);
        }
    }

    private int slowTick(final long nowMs)
    {
        int workCount = aeronClientInvoker.invoke();
        if (aeron.isClosed())
        {
            throw new AgentTerminationException("unexpected Aeron close");
        }

        if (nowMs >= markFileUpdateDeadlineMs)
        {
            markFile.updateActivityTimestamp(nowMs);
            markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
        }

        workCount += pollBackupArchiveEvents();

        if (NULL_VALUE == correlationId && null != clusterArchive)
        {
            clusterArchive.checkForErrorResponse();
        }

        return workCount;
    }

    private int resetBackup(final long nowMs)
    {
        timeOfLastProgressMs = nowMs;

        if (NULL_VALUE == coolDownDeadlineMs)
        {
            coolDownDeadlineMs = nowMs + coolDownIntervalMs;
            reset();
            return 1;
        }
        else if (nowMs > coolDownDeadlineMs)
        {
            coolDownDeadlineMs = NULL_VALUE;
            state(BACKUP_QUERY, nowMs);
            return 1;
        }

        return 0;
    }

    private int backupQuery(final long nowMs)
    {
        if (null == consensusPublication || nowMs > (timeOfLastBackupQueryMs + backupResponseTimeoutMs))
        {
            int cursor = ++clusterConsensusEndpointsCursor;
            if (cursor >= clusterConsensusEndpoints.length)
            {
                clusterConsensusEndpointsCursor = 0;
                cursor = 0;
            }

            CloseHelper.close(clusterArchiveAsyncConnect);
            clusterArchiveAsyncConnect = null;
            CloseHelper.close(clusterArchive);
            clusterArchive = null;

            final ChannelUri uri = ChannelUri.parse(ctx.consensusChannel());
            uri.put(ENDPOINT_PARAM_NAME, clusterConsensusEndpoints[cursor]);
            consensusPublication = aeron.addExclusivePublication(uri.toString(), ctx.consensusStreamId());
            correlationId = NULL_VALUE;
            timeOfLastBackupQueryMs = nowMs;

            return 1;
        }
        else if (NULL_VALUE == correlationId && consensusPublication.isConnected())
        {
            final long correlationId = aeron.nextCorrelationId();

            if (consensusPublisher.backupQuery(
                consensusPublication,
                correlationId,
                ctx.consensusStreamId(),
                AeronCluster.Configuration.PROTOCOL_SEMANTIC_VERSION,
                ctx.consensusChannel(),
                ArrayUtil.EMPTY_BYTE_ARRAY))
            {
                timeOfLastBackupQueryMs = nowMs;
                this.correlationId = correlationId;

                return 1;
            }
        }

        return 0;
    }

    private int snapshotRetrieve(final long nowMs)
    {
        int workCount = 0;

        if (null == clusterArchive)
        {
            final int step = clusterArchiveAsyncConnect.step();
            clusterArchive = clusterArchiveAsyncConnect.poll();
            return null == clusterArchive ? clusterArchiveAsyncConnect.step() - step : 1;
        }

        if (null == snapshotReplication)
        {
            final ChannelUri replicationUri = ChannelUri.parse(ctx.catchupChannel());
            replicationUri.put(ENDPOINT_PARAM_NAME, ctx.catchupEndpoint());
            final long replicationId = backupArchive.replicate(
                snapshotsToRetrieve.get(snapshotCursor).recordingId,
                NULL_VALUE,
                NULL_POSITION,
                clusterArchive.context().controlRequestStreamId(),
                clusterArchive.context().controlRequestChannel(),
                null,
                replicationUri.toString());

            snapshotReplication = new SnapshotReplication(replicationId);
            timeOfLastProgressMs = nowMs;
            workCount++;
        }
        else
        {
            workCount += pollBackupArchiveEvents();
            timeOfLastProgressMs = nowMs;

            if (snapshotReplication.isDone())
            {
                final RecordingLog.Snapshot snapshot = snapshotsToRetrieve.get(snapshotCursor);

                snapshotsRetrieved.add(new RecordingLog.Snapshot(
                    snapshotReplication.recordingId(),
                    snapshot.leadershipTermId,
                    snapshot.termBaseLogPosition,
                    snapshot.logPosition,
                    snapshot.timestamp,
                    snapshot.serviceId));

                snapshotReplication = null;

                if (++snapshotCursor >= snapshotsToRetrieve.size())
                {
                    state(LIVE_LOG_RECORD, nowMs);
                    workCount++;
                }
            }
            else
            {
                snapshotReplication.checkForError();
            }
        }

        return workCount;
    }

    private int liveLogRecord(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == liveLogRecordingSubscriptionId)
        {
            final String catchupEndpoint = ctx.catchupEndpoint();
            if (catchupEndpoint.endsWith(":0"))
            {
                if (null == recordingSubscription)
                {
                    final ChannelUri channelUri = ChannelUri.parse(ctx.catchupChannel());
                    channelUri.remove(ENDPOINT_PARAM_NAME);
                    channelUri.put(TAGS_PARAM_NAME, aeron.nextCorrelationId() + "," + aeron.nextCorrelationId());
                    recordingChannel = channelUri.toString();

                    final String channel = recordingChannel + "|endpoint=" + catchupEndpoint;
                    recordingSubscription = aeron.addSubscription(channel, ctx.logStreamId());
                    timeOfLastProgressMs = nowMs;
                    return 1;
                }
                else
                {
                    final String resolvedEndpoint = recordingSubscription.resolvedEndpoint();
                    if (null == resolvedEndpoint)
                    {
                        return 0;
                    }

                    final String endpoint = catchupEndpoint.substring(0, catchupEndpoint.length() - 2) +
                        resolvedEndpoint.substring(resolvedEndpoint.lastIndexOf(':'));
                    final ChannelUri channelUri = ChannelUri.parse(ctx.catchupChannel());
                    channelUri.put(ENDPOINT_PARAM_NAME, endpoint);
                    replayChannel = channelUri.toString();
                }
            }
            else
            {
                final ChannelUri channelUri = ChannelUri.parse(ctx.catchupChannel());
                channelUri.put(ENDPOINT_PARAM_NAME, catchupEndpoint);
                replayChannel = channelUri.toString();
                recordingChannel = replayChannel;
            }

            liveLogRecordingSubscriptionId = startLogRecording();
        }

        timeOfLastProgressMs = nowMs;
        state(LIVE_LOG_REPLAY, nowMs);
        workCount += 1;

        return workCount;
    }

    private int liveLogReplay(final long nowMs)
    {
        int workCount = 0;

        if (NULL_VALUE == liveLogRecordingId)
        {
            if (null == clusterArchive)
            {
                final int step = clusterArchiveAsyncConnect.step();
                clusterArchive = clusterArchiveAsyncConnect.poll();
                return null == clusterArchive ? clusterArchiveAsyncConnect.step() - step : 1;
            }

            if (NULL_VALUE == correlationId)
            {
                final RecordingLog.Entry logEntry = recordingLog.findLastTerm();
                final long startPosition = null == logEntry ?
                    NULL_POSITION : backupArchive.getStopPosition(logEntry.recordingId);
                final long replayId = ctx.aeron().nextCorrelationId();

                if (clusterArchive.archiveProxy().boundedReplay(
                    leaderLogRecordingId,
                    startPosition,
                    NULL_LENGTH,
                    leaderCommitPositionCounterId,
                    replayChannel,
                    ctx.logStreamId(),
                    replayId,
                    clusterArchive.controlSessionId()))
                {
                    replayChannel = null;
                    correlationId = replayId;
                    timeOfLastProgressMs = nowMs;
                    workCount++;
                }
            }
            else if (pollForResponse(clusterArchive, correlationId))
            {
                liveLogReplaySessionId = (int)clusterArchive.controlResponsePoller().relevantId();
                timeOfLastProgressMs = nowMs;
            }
            else if (NULL_COUNTER_ID == liveLogRecCounterId)
            {
                final CountersReader countersReader = aeron.countersReader();

                liveLogRecCounterId = RecordingPos.findCounterIdBySession(countersReader, liveLogReplaySessionId);
                if (NULL_COUNTER_ID != liveLogRecCounterId)
                {
                    liveLogPositionCounter.setOrdered(countersReader.getCounterValue(liveLogRecCounterId));
                    liveLogRecordingId = RecordingPos.getRecordingId(countersReader, liveLogRecCounterId);
                    timeOfLastBackupQueryMs = nowMs;
                    timeOfLastProgressMs = nowMs;

                    state(UPDATE_RECORDING_LOG, nowMs);
                }
            }
        }
        else
        {
            timeOfLastProgressMs = nowMs;
            state(UPDATE_RECORDING_LOG, nowMs);
        }

        return workCount;
    }

    private int updateRecordingLog(final long nowMs)
    {
        boolean wasRecordingLogUpdated = false;
        try
        {
            final long snapshotLeadershipTermId = snapshotsRetrieved.isEmpty() ?
                NULL_VALUE : snapshotsRetrieved.get(0).leadershipTermId;

            if (null != leaderLogEntry &&
                recordingLog.isUnknown(leaderLogEntry.leadershipTermId) &&
                leaderLogEntry.leadershipTermId <= snapshotLeadershipTermId)
            {
                recordingLog.appendTerm(
                    liveLogRecordingId,
                    leaderLogEntry.leadershipTermId,
                    leaderLogEntry.termBaseLogPosition,
                    leaderLogEntry.timestamp);

                wasRecordingLogUpdated = true;
                leaderLogEntry = null;
            }

            if (!snapshotsRetrieved.isEmpty())
            {
                for (int i = snapshotsRetrieved.size() - 1; i >= 0; i--)
                {
                    final RecordingLog.Snapshot snapshot = snapshotsRetrieved.get(i);

                    recordingLog.appendSnapshot(
                        snapshot.recordingId,
                        snapshot.leadershipTermId,
                        snapshot.termBaseLogPosition,
                        snapshot.logPosition,
                        snapshot.timestamp,
                        snapshot.serviceId);
                }

                wasRecordingLogUpdated = true;
            }

            if (null != leaderLastTermEntry && recordingLog.isUnknown(leaderLastTermEntry.leadershipTermId))
            {
                recordingLog.appendTerm(
                    liveLogRecordingId,
                    leaderLastTermEntry.leadershipTermId,
                    leaderLastTermEntry.termBaseLogPosition,
                    leaderLastTermEntry.timestamp);

                wasRecordingLogUpdated = true;
                leaderLastTermEntry = null;
            }
        }
        catch (final Throwable ex)
        {
            ctx.countedErrorHandler().onError(ex);
            throw new AgentTerminationException("failed to update recording log", ex);
        }

        if (wasRecordingLogUpdated && null != eventsListener)
        {
            eventsListener.onUpdatedRecordingLog(recordingLog, snapshotsRetrieved);
        }

        snapshotsRetrieved.clear();
        snapshotsToRetrieve.clear();

        timeOfLastProgressMs = nowMs;
        nextQueryDeadlineMsCounter.setOrdered(nowMs + backupQueryIntervalMs);
        state(BACKING_UP, nowMs);

        return 1;
    }

    private int backingUp(final long nowMs)
    {
        int workCount = 0;

        if (nowMs > nextQueryDeadlineMsCounter.get())
        {
            timeOfLastBackupQueryMs = nowMs;
            timeOfLastProgressMs = nowMs;
            state(BACKUP_QUERY, nowMs);
            workCount += 1;
        }

        if (NULL_COUNTER_ID != liveLogRecCounterId)
        {
            final long liveLogPosition = aeron.countersReader().getCounterValue(liveLogRecCounterId);

            if (liveLogPositionCounter.proposeMaxOrdered(liveLogPosition))
            {
                if (null != eventsListener)
                {
                    eventsListener.onLiveLogProgress(liveLogRecordingId, liveLogRecCounterId, liveLogPosition);
                }

                workCount += 1;
            }
        }

        return workCount;
    }

    private void state(final ClusterBackup.State newState, final long nowMs)
    {
        stateChange(state, newState, nowMs);

        if (BACKUP_QUERY == newState && null != eventsListener)
        {
            eventsListener.onBackupQuery();
        }

        if (!stateCounter.isClosed())
        {
            stateCounter.setOrdered(newState.code());
        }

        state = newState;
        correlationId = NULL_VALUE;
    }

    private void stateChange(final ClusterBackup.State oldState, final ClusterBackup.State newState, final long nowMs)
    {
        //System.out.println("ClusterBackup: " + oldState + " -> " + newState + " nowMs=" + nowMs);
    }

    private static boolean pollForResponse(final AeronArchive archive, final long correlationId)
    {
        final ControlResponsePoller poller = archive.controlResponsePoller();

        if (poller.poll() > 0 && poller.isPollComplete())
        {
            if (poller.controlSessionId() == archive.controlSessionId())
            {
                final ControlResponseCode code = poller.code();
                if (ControlResponseCode.ERROR == code)
                {
                    throw new ArchiveException(poller.errorMessage(), (int)poller.relevantId(), poller.correlationId());
                }

                return ControlResponseCode.OK == code && poller.correlationId() == correlationId;
            }
        }

        return false;
    }

    private int pollBackupArchiveEvents()
    {
        int workCount = 0;

        if (null != backupArchive)
        {
            final RecordingSignalPoller poller = this.recordingSignalPoller;
            workCount += poller.poll();

            if (poller.isPollComplete())
            {
                final int templateId = poller.templateId();

                if (ControlResponseDecoder.TEMPLATE_ID == templateId && poller.code() == ControlResponseCode.ERROR)
                {
                    final ArchiveException ex = new ArchiveException(
                        poller.errorMessage(), (int)poller.relevantId(), poller.correlationId());

                    if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
                    {
                        ctx.countedErrorHandler().onError(ex);
                        throw new AgentTerminationException();
                    }
                    else
                    {
                        throw ex;
                    }
                }
                else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId && null != snapshotReplication)
                {
                    snapshotReplication.onSignal(
                        poller.correlationId(),
                        poller.recordingId(),
                        poller.recordingPosition(),
                        poller.recordingSignal());
                }
            }
            else if (0 == workCount && !poller.subscription().isConnected())
            {
                ctx.countedErrorHandler().onError(new ClusterException("local archive is not connected", WARN));
                throw new AgentTerminationException();
            }
        }

        return workCount;
    }

    private long startLogRecording()
    {
        final long recordingSubscriptionId;

        final RecordingLog.Entry logEntry = recordingLog.findLastTerm();
        if (null == logEntry)
        {
            recordingSubscriptionId = backupArchive.startRecording(
                recordingChannel, ctx.logStreamId(), SourceLocation.REMOTE, true);
        }
        else
        {
            recordingSubscriptionId = backupArchive.extendRecording(
                logEntry.recordingId, recordingChannel, ctx.logStreamId(), SourceLocation.REMOTE, true);
        }

        recordingChannel = null;
        CloseHelper.close(ctx.countedErrorHandler(), recordingSubscription);
        recordingSubscription = null;

        return recordingSubscriptionId;
    }

    private boolean hasProgressStalled(final long nowMs)
    {
        return (NULL_COUNTER_ID == liveLogRecCounterId) && (nowMs > (timeOfLastProgressMs + backupProgressTimeoutMs));
    }
}
