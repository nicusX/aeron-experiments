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
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.exceptions.TimeoutException;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.CountedErrorHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Random;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class ElectionTest
{
    private static final long RECORDING_ID = 600L;
    private static final int LOG_SESSION_ID = 777;
    private final Aeron aeron = mock(Aeron.class);
    private final Counter electionStateCounter = mock(Counter.class);
    private final Counter commitPositionCounter = mock(Counter.class);
    private final Subscription subscription = mock(Subscription.class);
    private final Image logImage = mock(Image.class);
    private final RecordingLog recordingLog = mock(RecordingLog.class);
    private final ClusterMarkFile clusterMarkFile = mock(ClusterMarkFile.class);
    private final ConsensusPublisher consensusPublisher = mock(ConsensusPublisher.class);
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);
    private final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);
    private final TestClusterClock clock = new TestClusterClock(NANOSECONDS);
    private final MutableLong markFileCandidateTermId = new MutableLong(-1);

    private final ConsensusModule.Context ctx = new ConsensusModule.Context()
        .aeron(aeron)
        .recordingLog(recordingLog)
        .clusterClock(clock)
        .random(new Random())
        .electionStateCounter(electionStateCounter)
        .commitPositionCounter(commitPositionCounter)
        .clusterMarkFile(clusterMarkFile)
        .countedErrorHandler(countedErrorHandler);

    @BeforeEach
    public void before()
    {
        when(aeron.addCounter(anyInt(), anyString())).thenReturn(electionStateCounter);
        when(aeron.addSubscription(anyString(), anyInt())).thenReturn(subscription);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);
        when(consensusModuleAgent.addLogPublication()).thenReturn(LOG_SESSION_ID);
        when(subscription.imageBySessionId(anyInt())).thenReturn(logImage);

        when(clusterMarkFile.candidateTermId()).thenAnswer((invocation) -> markFileCandidateTermId.get());
        when(clusterMarkFile.proposeMaxCandidateTermId(anyLong(), anyInt())).thenAnswer(
            (invocation) ->
            {
                final long candidateTermId = invocation.getArgument(0);
                final long existingCandidateTermId = markFileCandidateTermId.get();

                if (candidateTermId > existingCandidateTermId)
                {
                    markFileCandidateTermId.set(candidateTermId);
                    return candidateTermId;
                }

                return existingCandidateTermId;
            });
    }

    @Test
    public void shouldElectSingleNodeClusterLeader()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint");

        final ClusterMember thisMember = clusterMembers[0];
        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, thisMember);

        final long newLeadershipTermId = leadershipTermId + 1;
        when(recordingLog.isUnknown(newLeadershipTermId)).thenReturn(Boolean.TRUE);
        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());

        verify(consensusModuleAgent).joinLogAsLeader(eq(newLeadershipTermId), eq(logPosition), anyInt(), eq(true));
        verify(recordingLog).isUnknown(newLeadershipTermId);
        verify(recordingLog).appendTerm(RECORDING_ID, newLeadershipTermId, logPosition, clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());
    }

    @Test
    @SuppressWarnings("MethodLength")
    public void shouldElectAppointedLeader()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[0];
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);

        ctx.appointedLeaderId(candidateMember.id());

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        final long candidateTermId = leadershipTermId + 1;
        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 1);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());
        verify(consensusPublisher).requestVote(
            clusterMembers[1].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(consensusPublisher).requestVote(
            clusterMembers[2].publication(),
            leadershipTermId,
            logPosition,
            candidateTermId,
            candidateMember.id());
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());
        verify(consensusModuleAgent).role(Cluster.Role.CANDIDATE);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[1].id(), true);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_LOG_REPLICATION.code());

        election.doWork(clock.nanoTime());

        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            leadershipTermId,
            0,
            0,
            NULL_POSITION,
            candidateTermId,
            logPosition,
            logPosition,
            RECORDING_ID,
            clock.nanoTime(),
            candidateMember.id(),
            LOG_SESSION_ID,
            election.isLeaderStartup());

        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            leadershipTermId,
            0,
            0,
            NULL_POSITION,
            candidateTermId,
            logPosition,
            logPosition,
            RECORDING_ID,
            clock.nanoTime(),
            candidateMember.id(),
            LOG_SESSION_ID,
            election.isLeaderStartup());

        when(recordingLog.isUnknown(candidateTermId)).thenReturn(Boolean.TRUE);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());

        election.doWork(clock.nanoTime());

        verify(consensusModuleAgent).joinLogAsLeader(eq(candidateTermId), eq(logPosition), anyInt(), eq(true));
        verify(recordingLog).appendTerm(RECORDING_ID, candidateTermId, logPosition, clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());

        assertEquals(NULL_POSITION, clusterMembers[1].logPosition());
        assertEquals(NULL_POSITION, clusterMembers[2].logPosition());
        assertEquals(candidateTermId, election.leadershipTermId());

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());

        when(consensusModuleAgent.electionComplete()).thenReturn(true);

        clock.increment(1);
        election.onAppendPosition(candidateTermId, logPosition, clusterMembers[1].id());
        election.onAppendPosition(candidateTermId, logPosition, clusterMembers[2].id());
        election.doWork(clock.nanoTime());
        final InOrder inOrder = inOrder(consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusModuleAgent).electionComplete();
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CLOSED.code());
    }

    @Test
    public void shouldVoteForAppointedLeader()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final int candidateId = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];
        final long leaderRecordingId = 983724;

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        long nowNs = 0;
        election.doWork(++nowNs);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, candidateId);
        verify(consensusPublisher).placeVote(
            clusterMembers[candidateId].publication(),
            candidateTermId,
            leadershipTermId,
            logPosition,
            candidateId,
            followerMember.id(),
            true);
        election.doWork(++nowNs);
        clock.increment(1);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        final int logSessionId = -7;
        election.onNewLeadershipTerm(
            leadershipTermId,
            NULL_VALUE,
            NULL_POSITION,
            NULL_POSITION,
            candidateTermId,
            logPosition,
            logPosition,
            leaderRecordingId,
            clock.nanoTime(),
            candidateId,
            logSessionId, false);

        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_REPLAY.code());

        election.doWork(++nowNs);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_INIT.code());

        election.doWork(++nowNs);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_AWAIT.code());

        when(consensusModuleAgent.tryJoinLogAsFollower(any(), anyBoolean())).thenReturn(true);
        election.doWork(++nowNs);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_READY.code());

        when(consensusPublisher.appendPosition(any(), anyLong(), anyLong(), anyInt())).thenReturn(Boolean.TRUE);
        when(consensusModuleAgent.electionComplete()).thenReturn(true);

        election.doWork(++nowNs);
        final InOrder inOrder = inOrder(consensusPublisher, consensusModuleAgent, electionStateCounter);
        inOrder.verify(consensusPublisher).appendPosition(
            clusterMembers[candidateId].publication(), candidateTermId, logPosition, followerMember.id());
        inOrder.verify(consensusModuleAgent).electionComplete();
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CLOSED.code());
    }

    @Test
    public void shouldCanvassMembersInSuccessfulLeadershipBid()
    {
        final long logPosition = 0;
        final long leadershipTermId = NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        election.doWork(0);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        verify(consensusPublisher).canvassPosition(
            clusterMembers[0].publication(), leadershipTermId, logPosition, leadershipTermId, followerMember.id());
        verify(consensusPublisher).canvassPosition(
            clusterMembers[2].publication(), leadershipTermId, logPosition, leadershipTermId, followerMember.id());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());
    }

    @Test
    public void shouldVoteForCandidateDuringNomination()
    {
        final long logPosition = 0;
        final long leadershipTermId = NULL_VALUE;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        clock.increment(1);
        election.doWork(clock.nanoTime());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(1);
        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());
    }

    @Test
    public void shouldTimeoutCanvassWithMajority()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onAppendPosition(leadershipTermId, logPosition, 0);

        clock.increment(1);
        election.doWork(clock.nanoTime());

        clock.increment(ctx.startupCanvassTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());
    }

    @Test
    public void shouldWinCandidateBallotWithMajority()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(ctx.electionTimeoutNs());
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_LOG_REPLICATION.code());
    }

    @ParameterizedTest
    @CsvSource(value = {"true,true", "true,false", "false,false", "false,true"})
    public void shouldBaseStartupValueOnLeader(final boolean isLeaderStart, final boolean isNodeStart)
    {
        final long leadershipTermId = 0;
        final long logPosition = 0;
        final long leaderRecordingId = 367234;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];
        when(consensusModuleAgent.tryJoinLogAsFollower(any(), anyBoolean())).thenReturn(true);

        final Election election = newElection(
            isNodeStart, leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        clock.increment(1);
        final int leaderMemberId = clusterMembers[0].id();
        election.onNewLeadershipTerm(
            leadershipTermId,
            NULL_VALUE,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            logPosition,
            logPosition,
            leaderRecordingId,
            clock.nanoTime(),
            leaderMemberId,
            0,
            isLeaderStart);
        election.doWork(clock.nanoTime());

        election.doWork(clock.increment(1));

        election.doWork(clock.increment(1));

        election.doWork(clock.increment(1));

        verify(consensusModuleAgent).tryJoinLogAsFollower(logImage, isLeaderStart);
    }

    @Test
    public void shouldElectCandidateWithFullVote()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(1);
        final long candidateTermId = leadershipTermId + 1;
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[0].id(), false);
        election.onVote(
            candidateTermId, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), true);
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_LOG_REPLICATION.code());
    }

    @Test
    public void shouldTimeoutCandidateBallotWithoutMajority()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);
        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 2);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        assertEquals(leadershipTermId, election.leadershipTermId());
    }

    @Test
    public void shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember candidateMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, candidateMember);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);

        clock.increment(ctx.startupCanvassTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs() >> 1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        clock.increment(1);
        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.CANDIDATE);
        election.onVote(
            leadershipTermId + 1, leadershipTermId, logPosition, candidateMember.id(), clusterMembers[2].id(), false);
        election.doWork(clock.nanoTime());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, logPosition, leadershipTermId, 0);

        election.doWork(clock.increment(1));

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        election.doWork(clock.increment(1));

        final long candidateTermId = leadershipTermId + 2;
        election.onVote(
            candidateTermId, leadershipTermId + 1, logPosition, candidateMember.id(), clusterMembers[2].id(), true);

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.LEADER_LOG_REPLICATION.code());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());

        clock.increment(1);
        election.doWork(clock.nanoTime());
        election.doWork(clock.nanoTime());
        assertEquals(candidateTermId, election.leadershipTermId());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.LEADER_READY.code());
    }

    @Test
    public void shouldTimeoutFollowerBallotWithoutLeaderEmerging()
    {
        final long leadershipTermId = NULL_VALUE;
        final long logPosition = 0L;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember followerMember = clusterMembers[1];

        final Election election = newElection(leadershipTermId, logPosition, clusterMembers, followerMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        final InOrder inOrder = Mockito.inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        final long candidateTermId = leadershipTermId + 1;
        election.onRequestVote(leadershipTermId, logPosition, candidateTermId, 0);

        clock.increment(1);
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        clock.increment(ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        assertEquals(leadershipTermId, election.leadershipTermId());
    }

    @Test
    public void shouldBecomeFollowerIfEnteringNewElection()
    {
        final long leadershipTermId = 1;
        final long logPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[0];

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.LEADER);
        final Election election = newElection(false, leadershipTermId, logPosition, clusterMembers, thisMember);

        clock.update(1, clock.timeUnit());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        verify(consensusModuleAgent).prepareForNewLeadership(logPosition);
        verify(consensusModuleAgent, atLeastOnce()).role(Cluster.Role.FOLLOWER);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void followerShouldReplicateLogBeforeReplayDuringElection()
    {
        final long term0Id = 0;
        final long term1Id = 1;
        final long term1BaseLogPosition = 60;

        final long term2Id = 2;
        final long term2BaseLogPosition = 120;

        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplication logReplication = mock(LogReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong())).thenReturn(term1BaseLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election followerElection = new Election(
            true,
            term0Id,
            term1BaseLogPosition,
            term1BaseLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = System.nanoTime();
        followerElection.doWork(++t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());
        reset(consensusPublisher);

        followerElection.onRequestVote(term1Id, term2BaseLogPosition, term2Id, leaderId);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        followerElection.onNewLeadershipTerm(
            term1Id,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        verify(electionStateCounter, times(2)).setOrdered(ElectionState.CANVASS.code());

        followerElection.doWork(++t1);
        verify(consensusPublisher).canvassPosition(
            liveLeader.publication(),
            term0Id,
            term1BaseLogPosition,
            term0Id,
            thisMember.id());

        followerElection.onNewLeadershipTerm(
            term0Id,
            term1Id,
            term1BaseLogPosition,
            term2BaseLogPosition,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        when(consensusModuleAgent.newLogReplication(any(), anyLong(), anyLong(), anyLong())).thenReturn(logReplication);
        followerElection.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), RECORDING_ID, term2BaseLogPosition, t1);

        when(logReplication.isDone(anyLong())).thenReturn(false);
        followerElection.doWork(++t1);
        followerElection.doWork(++t1);
        followerElection.doWork(++t1);
        followerElection.doWork(++t1);

        verify(consensusModuleAgent, times(4)).pollArchiveEvents();

        when(logReplication.isDone(anyLong())).thenReturn(true);
        when(logReplication.position()).thenReturn(term2BaseLogPosition);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(
            liveLeader.publication(), term2Id, term2BaseLogPosition, thisMember.id()))
            .thenReturn(true);
        t1 += ctx.leaderHeartbeatIntervalNs();

        verify(electionStateCounter, times(2)).setOrdered(ElectionState.CANVASS.code());
        followerElection.doWork(clock.nanoTime());

        verify(consensusPublisher).appendPosition(
            liveLeader.publication(), term2Id, term2BaseLogPosition, thisMember.id());
        verify(electionStateCounter, times(2)).setOrdered(ElectionState.CANVASS.code());

        followerElection.onCommitPosition(term2Id, term2BaseLogPosition, leaderId);
        followerElection.doWork(++t1);
        verify(electionStateCounter, times(3)).setOrdered(ElectionState.CANVASS.code());
    }

    @Test
    void followerShouldTimeoutLeaderIfReplicateLogPositionIsNotCommittedByLeader()
    {
        final long term1Id = 1;
        final long term2Id = 2;
        final long term1BaseLogPosition = 60;
        final long term2BaseLogPosition = 120;
        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplication logReplication = mock(LogReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong())).thenReturn(term1BaseLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            term1Id,
            term1BaseLogPosition,
            term1BaseLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        election.doWork(clock.increment(1));
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onRequestVote(term1Id, term2BaseLogPosition, term2Id, leaderId);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        election.onNewLeadershipTerm(
            term1Id,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            term2Id,
            term2BaseLogPosition,
            term2BaseLogPosition,
            RECORDING_ID,
            clock.nanoTime(),
            leaderId,
            0,
            true);

        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), anyLong(), anyLong(), anyLong())).thenReturn(logReplication);
        election.doWork(clock.increment(1));

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), RECORDING_ID, term2BaseLogPosition, clock.nanoTime());

        when(logReplication.isDone(anyLong())).thenReturn(true);
        when(logReplication.position()).thenReturn(term2BaseLogPosition);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(
            liveLeader.publication(), term1Id, term2BaseLogPosition, thisMember.id()))
            .thenReturn(true);
        clock.increment(ctx.leaderHeartbeatIntervalNs());
        election.doWork(clock.nanoTime());

        verify(consensusModuleAgent, atLeastOnce()).pollArchiveEvents();
        verify(consensusPublisher).appendPosition(
            liveLeader.publication(), term2Id, term2BaseLogPosition, thisMember.id());
        verify(electionStateCounter, never()).setOrdered(ElectionState.FOLLOWER_REPLAY.code());
        reset(countedErrorHandler);

        clock.increment(ctx.leaderHeartbeatTimeoutNs());
        assertThrows(TimeoutException.class, () -> election.doWork(clock.nanoTime()));
    }

    @Test
    void followerShouldProgressThroughFailedElectionsTermsImmediatelyPriorToCurrent()
    {
        final long term1Id = 1;
        final long term10Id = 10;
        final long term1BaseLogPosition = 60;
        final long term10BaseLogPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplication logReplication = mock(LogReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong())).thenReturn(term1BaseLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            term1Id,
            term1BaseLogPosition,
            term1BaseLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onRequestVote(term1Id, term10BaseLogPosition, term10Id, leaderId);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());

        election.onNewLeadershipTerm(
            term1Id,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), anyLong(), anyLong(), anyLong())).thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), RECORDING_ID, term10BaseLogPosition, t1);
    }

    @Test
    @SuppressWarnings("MethodLength")
    void followerShouldProgressThroughInterimElectionsTerms()
    {
        final long term9Id = 9;
        final long term10Id = 10;
        final long term9BaseLogPosition = 60;
        final long term10BaseLogPosition = 120;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplication logReplication = mock(LogReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong())).thenReturn(0L);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            0,
            0,
            0,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = System.nanoTime();
        election.doWork(++t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onRequestVote(term9Id, term10BaseLogPosition, term10Id, leaderId);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_BALLOT.code());
        reset(consensusPublisher);
        reset(electionStateCounter);

        election.onNewLeadershipTerm(
            term9Id,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.doWork(++t1);
        verify(consensusPublisher).canvassPosition(liveLeader.publication(), 0L, 0L, 0L, thisMember.id());

        for (int i = 0; i < term9Id - 1; i++)
        {
            reset(consensusPublisher);
            reset(electionStateCounter);

            election.onNewLeadershipTerm(
                i,
                i + 1,
                0,
                0,
                term10Id,
                term10BaseLogPosition,
                term10BaseLogPosition,
                RECORDING_ID,
                t1,
                leaderId,
                0,
                true);

            election.doWork(++t1);
            verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_REPLICATION.code());

            election.doWork(++t1);
            verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

            election.doWork(++t1);
            verify(consensusPublisher).canvassPosition(liveLeader.publication(), i + 1, 0L, term10Id, thisMember.id());
        }
        reset(consensusPublisher);
        reset(electionStateCounter);

        election.onNewLeadershipTerm(
            term9Id - 1,
            term9Id,
            term9BaseLogPosition,
            term10BaseLogPosition,
            term10Id,
            term10BaseLogPosition,
            term10BaseLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            true);

        election.doWork(++t1);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), anyLong(), anyLong(), anyLong())).thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent).newLogReplication(
            liveLeader.archiveEndpoint(), RECORDING_ID, term9BaseLogPosition, t1);
    }

    @Test
    void followerShouldReplayAndCatchupWhenLateJoiningClusterInSameTerm()
    {
        final long leadershipTermId = 1;
        final long leaderLogPosition = 120;
        final long followerLogPosition = 60;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplay logReplay = mock(LogReplay.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(consensusModuleAgent.prepareForNewLeadership(anyLong())).thenReturn(followerLogPosition);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            1,
            followerLogPosition,
            followerLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onNewLeadershipTerm(
            leadershipTermId,
            NULL_VALUE,
            NULL_POSITION,
            NULL_POSITION,
            leadershipTermId,
            followerLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            LOG_SESSION_ID,
            true);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_REPLAY.code());

        when(consensusModuleAgent.newLogReplay(anyLong(), anyLong())).thenReturn(logReplay);
        when(logReplay.isDone()).thenReturn(true);
        election.doWork(++t1);
        election.doWork(++t1);

        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_CATCHUP_INIT.code());
    }

    @Test
    void followerShouldReplicateReplayAndCatchupWhenLateJoiningClusterInLaterTerm()
    {
        final long leadershipTermId = 1;
        final long leaderLogPosition = 120;
        final long termBaseLogPosition = 60;
        final long localRecordingId = 2390485;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[1];
        final ClusterMember liveLeader = clusterMembers[0];
        final int leaderId = liveLeader.id();
        final LogReplication logReplication = mock(LogReplication.class);

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.FOLLOWER);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            0,
            0,
            0,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        long t1 = 0;
        election.doWork(++t1);
        verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onNewLeadershipTerm(
            0,
            1,
            termBaseLogPosition,
            leaderLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            t1,
            leaderId,
            0,
            false);
        verify(electionStateCounter).setOrdered(ElectionState.FOLLOWER_LOG_REPLICATION.code());

        when(consensusModuleAgent.newLogReplication(any(), anyLong(), anyLong(), anyLong())).thenReturn(logReplication);
        election.doWork(++t1);

        verify(consensusModuleAgent, times(1)).newLogReplication(
            liveLeader.archiveEndpoint(), RECORDING_ID, termBaseLogPosition, t1);

        when(logReplication.isDone(anyLong())).thenReturn(true);
        when(logReplication.position()).thenReturn(termBaseLogPosition);
        when(logReplication.recordingId()).thenReturn(localRecordingId);
        when(consensusPublisher.appendPosition(any(), anyLong(), anyLong(), anyInt())).thenReturn(true);
        t1 += ctx.leaderHeartbeatIntervalNs();
        election.doWork(++t1);

        verify(consensusPublisher).appendPosition(
            liveLeader.publication(), leadershipTermId, termBaseLogPosition, thisMember.id());

        election.onCommitPosition(leadershipTermId, leaderLogPosition, leaderId);
        election.doWork(++t1);
        verify(electionStateCounter, times(2)).setOrdered(ElectionState.CANVASS.code());
    }

    @Test
    void leaderShouldMoveToLogReplicationThenWaitForCommitPosition()
    {
        final long leadershipTermId = 1;
        final long candidateTermId = leadershipTermId + 1;
        final long leaderLogPosition = 120;
        final long followerLogPosition = 60;
        final ClusterMember[] clusterMembers = prepareClusterMembers();
        final ClusterMember thisMember = clusterMembers[0];
        final ClusterMember liveFollower = clusterMembers[1];
        final int leaderId = thisMember.id();
        final int followerId = liveFollower.id();

        when(consensusModuleAgent.role()).thenReturn(Cluster.Role.LEADER);
        when(consensusModuleAgent.logRecordingId()).thenReturn(RECORDING_ID);

        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();
        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        final Election election = new Election(
            true,
            leadershipTermId,
            leaderLogPosition,
            leaderLogPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);

        election.doWork(0);

        final InOrder inOrder = inOrder(electionStateCounter);
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.INIT.code());
        inOrder.verify(electionStateCounter).setOrdered(ElectionState.CANVASS.code());

        election.onCanvassPosition(leadershipTermId, leaderLogPosition, leadershipTermId, leaderId);
        election.onCanvassPosition(leadershipTermId, followerLogPosition, leadershipTermId, followerId);

        clock.increment(2 * ctx.startupCanvassTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.NOMINATE.code());

        clock.increment(2 * ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.CANDIDATE_BALLOT.code());

        election.onVote(candidateTermId, leadershipTermId, 120, leaderId, leaderId, true);
        election.onVote(candidateTermId, leadershipTermId, 120, leaderId, followerId, true);

        clock.increment(2 * ctx.electionTimeoutNs());
        election.doWork(clock.nanoTime());
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_LOG_REPLICATION.code());

        // Until the commit position moves to the leader's append position
        // we stay in the same state and emit new leadership terms.
        when(commitPositionCounter.getWeak()).thenReturn(followerLogPosition);
        election.doWork(clock.increment(1));
        verifyNoMoreInteractions(electionStateCounter);
        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[1].publication(),
            leadershipTermId,
            candidateTermId,
            leaderLogPosition,
            NULL_POSITION,
            candidateTermId,
            leaderLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            clock.nanoTime(),
            leaderId,
            LOG_SESSION_ID,
            election.isLeaderStartup());

        verify(consensusPublisher).newLeadershipTerm(
            clusterMembers[2].publication(),
            leadershipTermId,
            candidateTermId,
            leaderLogPosition,
            NULL_POSITION,
            candidateTermId,
            leaderLogPosition,
            leaderLogPosition,
            RECORDING_ID,
            clock.nanoTime(),
            leaderId,
            LOG_SESSION_ID,
            election.isLeaderStartup());

        // Begin replay once a quorum of followers have caught up.
        when(commitPositionCounter.getWeak()).thenReturn(leaderLogPosition);
        election.doWork(clock.increment(1));
        verify(electionStateCounter).setOrdered(ElectionState.LEADER_REPLAY.code());
    }

    private Election newElection(
        final boolean isStartup,
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        final Int2ObjectHashMap<ClusterMember> idToClusterMemberMap = new Int2ObjectHashMap<>();

        ClusterMember.addClusterMemberIds(clusterMembers, idToClusterMemberMap);

        return new Election(
            isStartup,
            logLeadershipTermId,
            logPosition,
            logPosition,
            clusterMembers,
            idToClusterMemberMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private Election newElection(
        final long logLeadershipTermId,
        final long logPosition,
        final ClusterMember[] clusterMembers,
        final ClusterMember thisMember)
    {
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap = new Int2ObjectHashMap<>();

        ClusterMember.addClusterMemberIds(clusterMembers, clusterMemberByIdMap);

        return new Election(
            true,
            logLeadershipTermId,
            logPosition,
            logPosition,
            clusterMembers,
            clusterMemberByIdMap,
            thisMember,
            consensusPublisher,
            ctx,
            consensusModuleAgent);
    }

    private static ClusterMember[] prepareClusterMembers()
    {
        final ClusterMember[] clusterMembers = ClusterMember.parse(
            "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
            "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
            "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");

        clusterMembers[0].publication(mock(ExclusivePublication.class));
        clusterMembers[1].publication(mock(ExclusivePublication.class));
        clusterMembers[2].publication(mock(ExclusivePublication.class));

        return clusterMembers;
    }
}
