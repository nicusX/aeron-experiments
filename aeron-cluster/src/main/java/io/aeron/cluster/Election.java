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

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.ChangeType;
import io.aeron.cluster.service.Cluster;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.AgentTerminationException;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.compareLog;
import static io.aeron.cluster.ElectionState.*;

/**
 * Election process to determine a new cluster leader and catch up followers.
 */
class Election
{
    private final boolean isNodeStartup;
    private boolean isFirstInit = true;
    private boolean isLeaderStartup;
    private boolean isExtendedCanvass;
    private int logSessionId = CommonContext.NULL_SESSION_ID;
    private long timeOfLastStateChangeNs;
    private long timeOfLastUpdateNs;
    private final long initialTimeOfLastUpdateNs;
    private long nominationDeadlineNs;
    private long logPosition;
    private long appendPosition;
    private long catchupPosition = NULL_POSITION;
    private long replicationLeadershipTermId = NULL_VALUE;
    private long replicationStopPosition = NULL_POSITION;
    private long leaderRecordingId = NULL_VALUE;
    private long leadershipTermId;
    private long logLeadershipTermId;
    private long candidateTermId;
    private ClusterMember leaderMember = null;
    private ElectionState state = INIT;
    private Subscription logSubscription = null;
    private LogReplay logReplay = null;
    private ClusterMember[] clusterMembers;
    private final ClusterMember thisMember;
    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap;
    private final ConsensusPublisher consensusPublisher;
    private final ConsensusModule.Context ctx;
    private final ConsensusModuleAgent consensusModuleAgent;
    private LogReplication logReplication = null;
    private long replicationCommitPosition = 0;
    private long replicationCommitPositionDeadlineNs = 0;

    Election(
        final boolean isNodeStartup,
        final long leadershipTermId,
        final long logPosition,
        final long appendPosition,
        final ClusterMember[] clusterMembers,
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap,
        final ClusterMember thisMember,
        final ConsensusPublisher consensusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        this.isNodeStartup = isNodeStartup;
        this.isExtendedCanvass = isNodeStartup;
        this.logPosition = logPosition;
        this.appendPosition = appendPosition;
        this.logLeadershipTermId = leadershipTermId;
        this.leadershipTermId = leadershipTermId;
        this.candidateTermId = leadershipTermId;
        this.clusterMembers = clusterMembers;
        this.clusterMemberByIdMap = clusterMemberByIdMap;
        this.thisMember = thisMember;
        this.consensusPublisher = consensusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;

        final long nowNs = ctx.clusterClock().timeNanos();
        this.initialTimeOfLastUpdateNs = nowNs - TimeUnit.DAYS.toNanos(1);
        this.timeOfLastUpdateNs = initialTimeOfLastUpdateNs;

        Objects.requireNonNull(thisMember);
        ctx.electionStateCounter().setOrdered(INIT.code());

        if (clusterMembers.length == 1 && thisMember.id() == clusterMembers[0].id())
        {
            candidateTermId = Math.max(leadershipTermId + 1, ctx.clusterMarkFile().candidateTermId() + 1);
            this.leadershipTermId = candidateTermId;
            leaderMember = thisMember;
            ctx.clusterMarkFile().candidateTermId(candidateTermId, ctx.fileSyncLevel());
            state(LEADER_LOG_REPLICATION, nowNs);
        }
    }

    ClusterMember leader()
    {
        return leaderMember;
    }

    long leadershipTermId()
    {
        return leadershipTermId;
    }

    long logPosition()
    {
        return logPosition;
    }

    boolean isLeaderStartup()
    {
        return isLeaderStartup;
    }

    int doWork(final long nowNs)
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                workCount += init(nowNs);
                break;

            case CANVASS:
                workCount += canvass(nowNs);
                break;

            case NOMINATE:
                workCount += nominate(nowNs);
                break;

            case CANDIDATE_BALLOT:
                workCount += candidateBallot(nowNs);
                break;

            case FOLLOWER_BALLOT:
                workCount += followerBallot(nowNs);
                break;

            case LEADER_LOG_REPLICATION:
                workCount += leaderLogReplication(nowNs);
                break;

            case LEADER_REPLAY:
                workCount += leaderReplay(nowNs);
                break;

            case LEADER_INIT:
                workCount += leaderInit(nowNs);
                break;

            case LEADER_READY:
                workCount += leaderReady(nowNs);
                break;

            case FOLLOWER_LOG_REPLICATION:
                workCount += followerLogReplication(nowNs);
                break;

            case FOLLOWER_REPLAY:
                workCount += followerReplay(nowNs);
                break;

            case FOLLOWER_CATCHUP_INIT:
                workCount += followerCatchupInit(nowNs);
                break;

            case FOLLOWER_CATCHUP_AWAIT:
                workCount += followerCatchupAwait(nowNs);
                break;

            case FOLLOWER_CATCHUP:
                workCount += followerCatchup(nowNs);
                break;

            case FOLLOWER_LOG_INIT:
                workCount += followerLogInit(nowNs);
                break;

            case FOLLOWER_LOG_AWAIT:
                workCount += followerLogAwait(nowNs);
                break;

            case FOLLOWER_READY:
                workCount += followerReady(nowNs);
                break;
        }

        return workCount;
    }

    void handleError(final long nowNs, final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
        logPosition = ctx.commitPositionCounter().getWeak();
        state(INIT, nowNs);

        if (ex instanceof AgentTerminationException || ex instanceof InterruptedException)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    void onRecordingSignal(
        final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (INIT == state)
        {
            return;
        }

        if (null != logReplication)
        {
            logReplication.onSignal(correlationId, recordingId, position, signal);
            consensusModuleAgent.logRecordingId(logReplication.recordingId());
        }
    }

    void onMembershipChange(
        final ClusterMember[] clusterMembers, final ChangeType changeType, final int memberId, final long logPosition)
    {
        if (INIT == state)
        {
            return;
        }

        ClusterMember.copyVotes(this.clusterMembers, clusterMembers);
        this.clusterMembers = clusterMembers;

        if (ChangeType.QUIT == changeType && FOLLOWER_CATCHUP == state && leaderMember.id() == memberId)
        {
            this.logPosition = logPosition;
            state(INIT, ctx.clusterClock().timeNanos());
        }
    }

    void onCanvassPosition(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId)
    {
        if (INIT == state)
        {
            return;
        }

        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower && thisMember.id() != followerMemberId)
        {
            follower
                .leadershipTermId(logLeadershipTermId)
                .logPosition(logPosition);

            if (logLeadershipTermId < this.leadershipTermId)
            {
                switch (state)
                {
                    case LEADER_LOG_REPLICATION:
                    case LEADER_INIT:
                    case LEADER_READY:
                    case LEADER_REPLAY:
                        publishNewLeadershipTerm(follower, logLeadershipTermId, ctx.clusterClock().timeNanos());
                        break;
                }
            }
            else if (logLeadershipTermId > this.leadershipTermId)
            {
                switch (state)
                {
                    case LEADER_LOG_REPLICATION:
                    case LEADER_READY:
                        throw new ClusterEvent("potential new election in progress");
                }
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId, final long logPosition, final long candidateTermId, final int candidateId)
    {
        if (INIT == state)
        {
            return;
        }

        if (isPassiveMember() || candidateId == thisMember.id())
        {
            return;
        }

        if (candidateTermId <= this.candidateTermId)
        {
            placeVote(candidateTermId, candidateId, false);
        }
        else if (compareLog(this.logLeadershipTermId, appendPosition, logLeadershipTermId, logPosition) > 0)
        {
            this.candidateTermId = ctx.clusterMarkFile().proposeMaxCandidateTermId(
                candidateTermId, ctx.fileSyncLevel());
            placeVote(candidateTermId, candidateId, false);
            state(INIT, ctx.clusterClock().timeNanos());
        }
        else if (CANVASS == state || NOMINATE == state || CANDIDATE_BALLOT == state || FOLLOWER_BALLOT == state)
        {
            this.candidateTermId = ctx.clusterMarkFile().proposeMaxCandidateTermId(
                candidateTermId, ctx.fileSyncLevel());
            placeVote(candidateTermId, candidateId, true);
            state(FOLLOWER_BALLOT, ctx.clusterClock().timeNanos());
        }
    }

    void onVote(
        final long candidateTermId,
        final long logLeadershipTermId,
        final long logPosition,
        final int candidateMemberId,
        final int followerMemberId,
        final boolean vote)
    {
        if (INIT == state)
        {
            return;
        }

        if (CANDIDATE_BALLOT == state &&
            candidateTermId == this.candidateTermId &&
            candidateMemberId == thisMember.id())
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .candidateTermId(candidateTermId)
                    .leadershipTermId(logLeadershipTermId)
                    .logPosition(logPosition)
                    .vote(vote ? Boolean.TRUE : Boolean.FALSE);
            }
        }
    }

    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        if (INIT == state)
        {
            return;
        }

        final ClusterMember leader = clusterMemberByIdMap.get(leaderMemberId);
        if (null == leader || (leaderMemberId == thisMember.id() && leadershipTermId == this.leadershipTermId))
        {
            return;
        }

        if (((FOLLOWER_BALLOT == state || CANDIDATE_BALLOT == state) && leadershipTermId == candidateTermId) ||
            CANVASS == state)
        {
            if (this.logLeadershipTermId == logLeadershipTermId)
            {
                if (NULL_POSITION != nextTermBaseLogPosition && nextTermBaseLogPosition < appendPosition)
                {
                    consensusModuleAgent.truncateLogEntry(logLeadershipTermId, nextTermBaseLogPosition);
                    appendPosition = consensusModuleAgent.prepareForNewLeadership(nextTermBaseLogPosition);
                }

                this.leaderMember = leader;
                this.isLeaderStartup = isStartup;
                this.leadershipTermId = leadershipTermId;
                this.candidateTermId = Math.max(leadershipTermId, candidateTermId);
                this.logSessionId = logSessionId;
                this.leaderRecordingId = leaderRecordingId;
                this.catchupPosition = appendPosition < logPosition ? logPosition : NULL_POSITION;

                if (this.appendPosition < termBaseLogPosition)
                {
                    if (NULL_VALUE != nextLeadershipTermId)
                    {
                        if (appendPosition < nextTermBaseLogPosition)
                        {
                            replicationLeadershipTermId = logLeadershipTermId;
                            replicationStopPosition = nextTermBaseLogPosition;
                            state(FOLLOWER_LOG_REPLICATION, ctx.clusterClock().timeNanos());
                        }
                        else if (appendPosition == nextTermBaseLogPosition)
                        {
                            if (NULL_POSITION != nextLogPosition)
                            {
                                replicationLeadershipTermId = nextLeadershipTermId;
                                replicationStopPosition = nextLogPosition;
                                state(FOLLOWER_LOG_REPLICATION, ctx.clusterClock().timeNanos());
                            }
                        }
                    }
                    else
                    {
                        throw new ClusterException(
                            "invalid newLeadershipTerm - this.appendPosition=" + appendPosition +
                            " < termBaseLogPosition = " + termBaseLogPosition +
                            " and nextLeadershipTermId = " + nextLeadershipTermId +
                            ", logLeadershipTermId = " + logLeadershipTermId +
                            ", nextTermBaseLogPosition = " + nextTermBaseLogPosition +
                            ", nextLogPosition = " + nextLogPosition + ", leadershipTermId = " + leadershipTermId +
                            ", termBaseLogPosition = " + termBaseLogPosition + ", logPosition = " + logPosition +
                            ", leaderRecordingId = " + leaderRecordingId + ", timestamp = " + timestamp +
                            ", leaderMemberId = " + leaderMemberId + ", logSessionId = " + logSessionId +
                            ", isStartup = " + isStartup);
                    }
                }
                else
                {
                    state(FOLLOWER_REPLAY, ctx.clusterClock().timeNanos());
                }
            }
            else
            {
                state(CANVASS, ctx.clusterClock().timeNanos());
            }
        }
    }

    void onAppendPosition(final long leadershipTermId, final long logPosition, final int followerMemberId)
    {
        if (INIT == state)
        {
            return;
        }

        if (leadershipTermId <= this.leadershipTermId)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                follower
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timeOfLastAppendPositionNs(ctx.clusterClock().timeNanos());

                consensusModuleAgent.trackCatchupCompletion(follower, leadershipTermId);
            }
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        if (INIT == state)
        {
            return;
        }

        if (leadershipTermId == this.leadershipTermId &&
            NULL_POSITION != catchupPosition &&
            FOLLOWER_CATCHUP == state &&
            leaderMemberId == leaderMember.id())
        {
            catchupPosition = Math.max(catchupPosition, logPosition);
        }
        else if (FOLLOWER_LOG_REPLICATION == state && leaderMemberId == leaderMember.id())
        {
            replicationCommitPosition = Math.max(replicationCommitPosition, logPosition);
            replicationCommitPositionDeadlineNs = ctx.clusterClock().timeNanos() + ctx.leaderHeartbeatTimeoutNs();
        }
        else if (leadershipTermId > this.leadershipTermId && LEADER_READY == state)
        {
            throw new ClusterEvent("new leader detected due to commit position");
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long logRecordingId,
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition)
    {
        if (INIT == state)
        {
            return;
        }

        if (FOLLOWER_CATCHUP == state || FOLLOWER_REPLAY == state)
        {
            for (long termId = Math.max(logLeadershipTermId, 0); termId <= leadershipTermId; termId++)
            {
                final RecordingLog recordingLog = ctx.recordingLog();
                if (!recordingLog.isUnknown(termId - 1))
                {
                    recordingLog.commitLogPosition(termId - 1, termBaseLogPosition);
                    recordingLog.force(ctx.fileSyncLevel());
                }

                if (recordingLog.isUnknown(termId))
                {
                    recordingLog.appendTerm(logRecordingId, termId, termBaseLogPosition, timestamp);
                    recordingLog.force(ctx.fileSyncLevel());
                }
            }

            this.logLeadershipTermId = leadershipTermId;
            this.logPosition = logPosition;
        }
    }

    private int init(final long nowNs)
    {
        if (isFirstInit)
        {
            isFirstInit = false;
            if (!isNodeStartup)
            {
                appendPosition = consensusModuleAgent.prepareForNewLeadership(logPosition);
            }
        }
        else
        {
            cleanupLogReplication();
            resetCatchup();

            appendPosition = consensusModuleAgent.prepareForNewLeadership(logPosition);
            logSessionId = CommonContext.NULL_SESSION_ID;
            cleanupReplay();
            CloseHelper.close(logSubscription);
            logSubscription = null;
        }

        candidateTermId = Math.max(ctx.clusterMarkFile().candidateTermId(), leadershipTermId);

        if (clusterMembers.length == 1 && thisMember.id() == clusterMembers[0].id())
        {
            state(LEADER_LOG_REPLICATION, nowNs);
        }
        else
        {
            state(CANVASS, nowNs);
        }

        return 1;
    }

    private int canvass(final long nowNs)
    {
        int workCount = 0;

        if (hasIntervalExpired(nowNs, ctx.electionStatusIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            for (final ClusterMember member : clusterMembers)
            {
                if (member.id() != thisMember.id())
                {
                    consensusPublisher.canvassPosition(
                        member.publication(), logLeadershipTermId, appendPosition, leadershipTermId, thisMember.id());
                }
            }

            workCount++;
        }

        if (isPassiveMember() || (ctx.appointedLeaderId() != NULL_VALUE && ctx.appointedLeaderId() != thisMember.id()))
        {
            return workCount;
        }

        final long canvassDeadlineNs =
            timeOfLastStateChangeNs + (isExtendedCanvass ? ctx.startupCanvassTimeoutNs() : ctx.electionTimeoutNs());

        if (ClusterMember.isUnanimousCandidate(clusterMembers, thisMember) ||
            (ClusterMember.isQuorumCandidate(clusterMembers, thisMember) && nowNs >= canvassDeadlineNs))
        {
            final long delayNs = (long)(ctx.random().nextDouble() * (ctx.electionTimeoutNs() >> 1));
            nominationDeadlineNs = nowNs + delayNs;
            state(NOMINATE, nowNs);
            workCount++;
        }

        return workCount;
    }

    private int nominate(final long nowNs)
    {
        if (nowNs >= nominationDeadlineNs)
        {
            candidateTermId = ctx.clusterMarkFile().proposeMaxCandidateTermId(
                candidateTermId + 1, ctx.fileSyncLevel());
            ClusterMember.becomeCandidate(clusterMembers, candidateTermId, thisMember.id());
            state(CANDIDATE_BALLOT, nowNs);
            return 1;
        }

        return 0;
    }

    private int candidateBallot(final long nowNs)
    {
        int workCount = 0;

        if (ClusterMember.hasWonVoteOnFullCount(clusterMembers, candidateTermId) ||
            ClusterMember.hasMajorityVoteWithCanvassMembers(clusterMembers, candidateTermId))
        {
            leaderMember = thisMember;
            leadershipTermId = candidateTermId;
            state(LEADER_LOG_REPLICATION, nowNs);
            workCount++;
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            if (ClusterMember.hasMajorityVote(clusterMembers, candidateTermId))
            {
                leaderMember = thisMember;
                leadershipTermId = candidateTermId;
                state(LEADER_LOG_REPLICATION, nowNs);
            }
            else
            {
                state(CANVASS, nowNs);
            }

            workCount++;
        }
        else
        {
            for (final ClusterMember member : clusterMembers)
            {
                if (!member.isBallotSent())
                {
                    workCount++;
                    member.isBallotSent(consensusPublisher.requestVote(
                        member.publication(), logLeadershipTermId, appendPosition, candidateTermId, thisMember.id()));
                }
            }
        }

        return workCount;
    }

    private int followerBallot(final long nowNs)
    {
        int workCount = 0;

        if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            state(CANVASS, nowNs);
            workCount++;
        }

        return workCount;
    }

    private int leaderLogReplication(final long nowNs)
    {
        int workCount = 0;

        workCount += consensusModuleAgent.updateLeaderPosition(nowNs, appendPosition);
        workCount += publishNewLeadershipTermOnInterval(nowNs);

        if (ctx.commitPositionCounter().getWeak() >= appendPosition)
        {
            workCount++;
            state(LEADER_REPLAY, nowNs);
        }

        return workCount;
    }

    private int leaderReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            if (logPosition < appendPosition)
            {
                ctx.commitPositionCounter().setOrdered(logPosition);
                logReplay = consensusModuleAgent.newLogReplay(logPosition, appendPosition);
            }
            else
            {
                state(LEADER_INIT, nowNs);
            }

            workCount++;
            isLeaderStartup = isNodeStartup;
            ClusterMember.resetLogPositions(clusterMembers, NULL_POSITION);
            thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition);
        }
        else
        {
            workCount += logReplay.doWork();
            if (logReplay.isDone())
            {
                cleanupReplay();
                logPosition = appendPosition;
                state(LEADER_INIT, nowNs);
            }
        }

        workCount += publishNewLeadershipTermOnInterval(nowNs);

        return workCount;
    }

    private int leaderInit(final long nowNs)
    {
        consensusModuleAgent.joinLogAsLeader(leadershipTermId, logPosition, logSessionId, isLeaderStartup);
        updateRecordingLog(nowNs);
        state(LEADER_READY, nowNs);

        return 1;
    }

    private int leaderReady(final long nowNs)
    {
        int workCount = consensusModuleAgent.updateLeaderPosition(nowNs, appendPosition);
        workCount += publishNewLeadershipTermOnInterval(nowNs);

        if (ClusterMember.haveVotersReachedPosition(clusterMembers, logPosition, leadershipTermId))
        {
            if (consensusModuleAgent.electionComplete())
            {
                state(CLOSED, nowNs);
                workCount++;
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()) &&
            ClusterMember.haveQuorumReachedPosition(clusterMembers, logPosition, leadershipTermId))
        {
            if (consensusModuleAgent.electionComplete())
            {
                state(CLOSED, nowNs);
                workCount++;
            }
        }

        return workCount;
    }

    private int followerLogReplication(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplication)
        {
            if (appendPosition < replicationStopPosition)
            {
                logReplication = consensusModuleAgent.newLogReplication(
                    leaderMember.archiveEndpoint(), leaderRecordingId, replicationStopPosition, nowNs);
                replicationCommitPositionDeadlineNs = nowNs + ctx.leaderHeartbeatTimeoutNs();
                workCount++;
            }
            else
            {
                updateRecordingLogForReplication(replicationLeadershipTermId, replicationStopPosition, nowNs);
                state(CANVASS, nowNs);
            }
        }
        else
        {
            workCount += consensusModuleAgent.pollArchiveEvents();
            workCount += publishFollowerReplicationPosition(nowNs);

            if (logReplication.isDone(nowNs))
            {
                if (replicationCommitPosition >= appendPosition)
                {
                    appendPosition = logReplication.position();
                    cleanupLogReplication();
                    updateRecordingLogForReplication(replicationLeadershipTermId, replicationStopPosition, nowNs);
                    state(CANVASS, nowNs);
                    workCount++;
                }
                else if (nowNs >= replicationCommitPositionDeadlineNs)
                {
                    throw new TimeoutException("timeout awaiting commit position", AeronException.Category.WARN);
                }
            }
        }

        return workCount;
    }

    private int followerReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            workCount++;
            if (logPosition < appendPosition)
            {
                logReplay = consensusModuleAgent.newLogReplay(logPosition, appendPosition);
            }
            else
            {
                state(NULL_POSITION != catchupPosition ? FOLLOWER_CATCHUP_INIT : FOLLOWER_LOG_INIT, nowNs);
            }
        }
        else
        {
            workCount += logReplay.doWork();
            if (logReplay.isDone())
            {
                cleanupReplay();
                logPosition = appendPosition;
                state(NULL_POSITION != catchupPosition ? FOLLOWER_CATCHUP_INIT : FOLLOWER_LOG_INIT, nowNs);
            }
        }

        return workCount;
    }

    private int followerCatchupInit(final long nowNs)
    {
        if (null == logSubscription)
        {
            logSubscription = addFollowerSubscription();
            addCatchupLogDestination();
        }

        String catchupEndpoint = null;
        final String endpoint = thisMember.catchupEndpoint();
        if (endpoint.endsWith(":0"))
        {
            final String resolvedEndpoint = logSubscription.resolvedEndpoint();
            if (null != resolvedEndpoint)
            {
                final int i = resolvedEndpoint.lastIndexOf(':');
                catchupEndpoint = endpoint.substring(0, endpoint.length() - 2) + resolvedEndpoint.substring(i);
            }
        }
        else
        {
            catchupEndpoint = endpoint;
        }

        if (null != catchupEndpoint && sendCatchupPosition(catchupEndpoint))
        {
            timeOfLastUpdateNs = nowNs;
            consensusModuleAgent.catchupInitiated(nowNs);
            state(FOLLOWER_CATCHUP_AWAIT, nowNs);
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to send catchup position", AeronException.Category.WARN);
        }

        return 1;
    }

    private int followerCatchupAwait(final long nowNs)
    {
        int workCount = 0;

        final Image image = logSubscription.imageBySessionId(logSessionId);
        if (null != image)
        {
            verifyLogImage(image);
            if (consensusModuleAgent.tryJoinLogAsFollower(image, isLeaderStartup))
            {
                state(FOLLOWER_CATCHUP, nowNs);
                workCount++;
            }
            else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
            {
                throw new TimeoutException("failed to join catchup log as follower", AeronException.Category.WARN);
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to join catchup log", AeronException.Category.WARN);
        }

        return workCount;
    }

    private int followerCatchup(final long nowNs)
    {
        int workCount = consensusModuleAgent.catchupPoll(catchupPosition, nowNs);

        if (null == consensusModuleAgent.liveLogDestination() &&
            consensusModuleAgent.isCatchupNearLive(catchupPosition))
        {
            addLiveLogDestination();
            workCount++;
        }

        final long position = ctx.commitPositionCounter().getWeak();
        if (position >= catchupPosition &&
            null == consensusModuleAgent.catchupLogDestination() &&
            ConsensusModule.State.SNAPSHOT != consensusModuleAgent.state())
        {
            appendPosition = position;
            logPosition = position;
            state(FOLLOWER_LOG_INIT, nowNs);
            workCount++;
        }

        return workCount;
    }

    private int followerLogInit(final long nowNs)
    {
        if (null == logSubscription)
        {
            if (CommonContext.NULL_SESSION_ID != logSessionId)
            {
                logSubscription = addFollowerSubscription();
                addLiveLogDestination();
                state(FOLLOWER_LOG_AWAIT, nowNs);
            }
        }
        else
        {
            state(FOLLOWER_READY, nowNs);
        }

        return 1;
    }

    private int followerLogAwait(final long nowNs)
    {
        int workCount = 0;

        final Image image = logSubscription.imageBySessionId(logSessionId);
        if (null != image)
        {
            verifyLogImage(image);
            if (consensusModuleAgent.tryJoinLogAsFollower(image, isLeaderStartup))
            {
                appendPosition = image.joinPosition();
                logPosition = image.joinPosition();
                updateRecordingLog(nowNs);
                state(FOLLOWER_READY, nowNs);
                workCount++;
            }
            else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
            {
                throw new TimeoutException("failed to join live log as follower", AeronException.Category.WARN);
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to join live log", AeronException.Category.WARN);
        }

        return workCount;
    }

    private int followerReady(final long nowNs)
    {
        if (consensusPublisher.appendPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id()))
        {
            consensusModuleAgent.leadershipTermId(leadershipTermId);
            if (consensusModuleAgent.electionComplete())
            {
                state(CLOSED, nowNs);
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("ready follower failed to notify leader", AeronException.Category.WARN);
        }

        return 1;
    }

    private void placeVote(final long candidateTermId, final int candidateId, final boolean vote)
    {
        final ClusterMember candidate = clusterMemberByIdMap.get(candidateId);
        if (null != candidate)
        {
            consensusPublisher.placeVote(
                candidate.publication(),
                candidateTermId,
                logLeadershipTermId,
                appendPosition,
                candidateId,
                thisMember.id(),
                vote);
        }
    }

    private int publishNewLeadershipTermOnInterval(final long nowNs)
    {
        int workCount = 0;

        if (hasIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishNewLeadershipTerm(ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS));
            workCount++;
        }

        return workCount;
    }

    private void publishNewLeadershipTerm(final long timestamp)
    {
        for (final ClusterMember member : clusterMembers)
        {
            publishNewLeadershipTerm(member, logLeadershipTermId, timestamp);
        }
    }

    private void publishNewLeadershipTerm(
        final ClusterMember member, final long logLeadershipTermId, final long timestamp)
    {
        if (member.id() != thisMember.id() && CommonContext.NULL_SESSION_ID != logSessionId)
        {
            final RecordingLog.Entry logNextTermEntry =
                ctx.recordingLog().findTermEntry(logLeadershipTermId + 1);

            final long nextLeadershipTermId = null != logNextTermEntry ?
                logNextTermEntry.leadershipTermId : leadershipTermId;
            final long nextTermBaseLogPosition = null != logNextTermEntry ?
                logNextTermEntry.termBaseLogPosition : appendPosition;

            final long nextLogPosition = null != logNextTermEntry ?
                (NULL_POSITION != logNextTermEntry.logPosition ? logNextTermEntry.logPosition : appendPosition) :
                NULL_POSITION;

            consensusPublisher.newLeadershipTerm(
                member.publication(),
                logLeadershipTermId,
                nextLeadershipTermId,
                nextTermBaseLogPosition,
                nextLogPosition,
                leadershipTermId,
                appendPosition,
                appendPosition,
                consensusModuleAgent.logRecordingId(),
                timestamp,
                thisMember.id(),
                logSessionId,
                isLeaderStartup);
        }
    }

    private int publishFollowerReplicationPosition(final long nowNs)
    {
        final long position = logReplication.position();
        if (position > appendPosition && hasIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs()))
        {
            if (consensusPublisher.appendPosition(
                leaderMember.publication(), leadershipTermId, position, thisMember.id()))
            {
                appendPosition = position;
                timeOfLastUpdateNs = nowNs;
                return 1;
            }
        }

        return 0;
    }

    private boolean sendCatchupPosition(final String catchupEndpoint)
    {
        return consensusPublisher.catchupPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id(), catchupEndpoint);
    }

    private void addCatchupLogDestination()
    {
        final String destination = "aeron:udp?endpoint=" + thisMember.catchupEndpoint();
        logSubscription.addDestination(destination);
        consensusModuleAgent.catchupLogDestination(destination);
    }

    private void addLiveLogDestination()
    {
        final String destination = ctx.isLogMdc() ? "aeron:udp?endpoint=" + thisMember.logEndpoint() : ctx.logChannel();
        logSubscription.addDestination(destination);
        consensusModuleAgent.liveLogDestination(destination);
    }

    private Subscription addFollowerSubscription()
    {
        final String channel = new ChannelUriStringBuilder()
            .media(CommonContext.UDP_MEDIA)
            .tags(ctx.aeron().nextCorrelationId() + "," + ctx.aeron().nextCorrelationId())
            .controlMode(CommonContext.MDC_CONTROL_MODE_MANUAL)
            .sessionId(logSessionId)
            .group(Boolean.TRUE)
            .rejoin(Boolean.FALSE)
            .alias("log")
            .build();

        return ctx.aeron().addSubscription(channel, ctx.logStreamId());
    }

    private void state(final ElectionState newState, final long nowNs)
    {
        if (newState != state)
        {
            stateChange(state, newState, thisMember.id());

            if (CANVASS == state)
            {
                isExtendedCanvass = false;
            }

            switch (newState)
            {
                case CANVASS:
                    resetMembers();
                    consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                    break;

                case CANDIDATE_BALLOT:
                    consensusModuleAgent.role(Cluster.Role.CANDIDATE);
                    break;

                case LEADER_LOG_REPLICATION:
                    logSessionId = consensusModuleAgent.addLogPublication();
                    break;

                case LEADER_INIT:
                    consensusModuleAgent.role(Cluster.Role.LEADER);
                    break;

                case FOLLOWER_LOG_REPLICATION:
                case FOLLOWER_REPLAY:
                    consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                    break;
            }

            state = newState;
            ctx.electionStateCounter().setOrdered(newState.code());
            timeOfLastStateChangeNs = nowNs;
            timeOfLastUpdateNs = initialTimeOfLastUpdateNs;
        }
    }

    private void resetCatchup()
    {
        consensusModuleAgent.stopAllCatchups();
        catchupPosition = NULL_POSITION;
    }

    private void resetMembers()
    {
        ClusterMember.reset(clusterMembers);
        thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition);
        leaderMember = null;
    }

    private void cleanupReplay()
    {
        if (null != logReplay)
        {
            logReplay.close();
            logReplay = null;
        }
    }

    private void cleanupLogReplication()
    {
        if (null != logReplication)
        {
            logReplication.close();
            logReplication = null;
        }
        replicationCommitPosition = 0;
        replicationCommitPositionDeadlineNs = 0;
    }

    private boolean isPassiveMember()
    {
        return null == ClusterMember.findMember(clusterMembers, thisMember.id());
    }

    private void updateRecordingLog(final long nowNs)
    {
        final RecordingLog recordingLog = ctx.recordingLog();
        final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);
        final long recordingId = consensusModuleAgent.logRecordingId();

        if (NULL_VALUE == recordingId)
        {
            throw new AgentTerminationException("log recording id not found");
        }

        for (long termId = logLeadershipTermId + 1; termId <= leadershipTermId; termId++)
        {
            if (recordingLog.isUnknown(termId))
            {
                recordingLog.appendTerm(recordingId, termId, logPosition, timestamp);
                recordingLog.force(ctx.fileSyncLevel());
                logLeadershipTermId = termId;
            }
        }
    }

    private void updateRecordingLogForReplication(final long leadershipTermId, final long logPosition, final long nowNs)
    {
        final RecordingLog recordingLog = ctx.recordingLog();
        final long timestamp = ctx.clusterClock().timeUnit().convert(nowNs, TimeUnit.NANOSECONDS);
        final long recordingId = consensusModuleAgent.logRecordingId();

        if (NULL_VALUE == recordingId)
        {
            throw new AgentTerminationException("log recording id not found");
        }

        for (long termId = logLeadershipTermId + 1; termId <= leadershipTermId; termId++)
        {
            if (recordingLog.isUnknown(termId))
            {
                final RecordingLog.Entry lastTerm = recordingLog.findLastTerm();
                final long termBaseLogPosition = null != lastTerm ? lastTerm.logPosition : 0;

                recordingLog.appendTerm(recordingId, termId, termBaseLogPosition, timestamp);
            }

            recordingLog.commitLogPosition(termId, logPosition);
            recordingLog.force(ctx.fileSyncLevel());
            logLeadershipTermId = termId;
        }
    }

    private void verifyLogImage(final Image image)
    {
        if (image.joinPosition() != logPosition)
        {
            throw new ClusterException(
                "joinPosition=" + image.joinPosition() + " != logPosition=" + logPosition,
                ClusterException.Category.WARN);
        }
    }

    private boolean hasIntervalExpired(final long nowNs, final long intervalNs)
    {
        return (nowNs - timeOfLastUpdateNs) >= intervalNs;
    }

    void stateChange(final ElectionState oldState, final ElectionState newState, final int memberId)
    {
        /*
        System.out.println("Election: memberId=" + memberId + " " + oldState + " -> " + newState +
            " leaderId=" + (null != leaderMember ? leaderMember.id() : -1) +
            " candidateTermId=" + candidateTermId +
            " leadershipTermId=" + leadershipTermId +
            " logPosition=" + logPosition +
            " logLeadershipTermId=" + logLeadershipTermId +
            " appendPosition=" + appendPosition +
            " catchupPosition=" + catchupPosition);
        */
    }

    public String toString()
    {
        return "Election{" +
            "isNodeStartup=" + isNodeStartup +
            ", isLeaderStartup=" + isLeaderStartup +
            ", isExtendedCanvass=" + isExtendedCanvass +
            ", logSessionId=" + logSessionId +
            ", timeOfLastStateChangeNs=" + timeOfLastStateChangeNs +
            ", timeOfLastUpdateNs=" + timeOfLastUpdateNs +
            ", nominationDeadlineNs=" + nominationDeadlineNs +
            ", logPosition=" + logPosition +
            ", appendPosition=" + appendPosition +
            ", catchupPosition=" + catchupPosition +
            ", logReplicationPosition=" + replicationStopPosition +
            ", leaderRecordingId=" + leaderRecordingId +
            ", leadershipTermId=" + leadershipTermId +
            ", logLeadershipTermId=" + logLeadershipTermId +
            ", candidateTermId=" + candidateTermId +
            ", leaderMember=" + leaderMember +
            ", state=" + state +
            ", logSubscription=" + logSubscription +
            ", logReplay=" + logReplay +
            ", clusterMembers=" + Arrays.toString(clusterMembers) +
            ", thisMember=" + thisMember +
            ", clusterMemberByIdMap=" + clusterMemberByIdMap +
            ", logReplication=" + logReplication +
            ", ctx=" + ctx +
            '}';
    }
}
