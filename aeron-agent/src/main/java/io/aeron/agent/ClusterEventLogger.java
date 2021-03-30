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
package io.aeron.agent;

import io.aeron.cluster.ConsensusModule;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.ClusterEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;

/**
 * Event logger interface used by interceptors for recording cluster events into a {@link RingBuffer} for a
 * {@link ConsensusModule} events via a Java Agent.
 */
public final class ClusterEventLogger
{
    /**
     * Logger for writing into the {@link EventConfiguration#EVENT_RING_BUFFER}.
     */
    public static final ClusterEventLogger LOGGER = new ClusterEventLogger(EVENT_RING_BUFFER);

    private final ManyToOneRingBuffer ringBuffer;

    ClusterEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    /**
     * Log a new leadership term event.
     *
     * @param logLeadershipTermId term for which log entries are present.
     * @param logTruncatePosition position of the log for the logLeadershipTermId.
     * @param leadershipTermId    new leadership term id.
     * @param termBaseLogPosition position the log reached at base of new term.
     * @param logPosition         position the log reached for the new term.
     * @param leaderRecordingId   of the log in the leader archive.
     * @param timestamp           of the the new term.
     * @param leaderMemberId      member id for the new leader.
     * @param logSessionId        session id of the log extension.
     * @param isStartup           is the leader starting up fresh.
     */
    public void logNewLeadershipTerm(
        final long logLeadershipTermId,
        final long logTruncatePosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        final int length = newLeaderShipTermLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(NEW_LEADERSHIP_TERM.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeNewLeadershipTerm(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    logLeadershipTermId,
                    logTruncatePosition,
                    leadershipTermId,
                    termBaseLogPosition,
                    logPosition,
                    leaderRecordingId,
                    timestamp,
                    leaderMemberId,
                    logSessionId,
                    isStartup);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a state change event for a cluster node.
     *
     * @param eventCode for the type of state change.
     * @param oldState  before the change.
     * @param newState  after the change.
     * @param memberId  on which the change has taken place.
     * @param <E> type representing the state change.
     */
    public <E extends Enum<E>> void logStateChange(
        final ClusterEventCode eventCode, final E oldState, final E newState, final int memberId)
    {
        final int length = stateChangeLength(oldState, newState);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(eventCode.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeStateChange(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    oldState,
                    newState,
                    memberId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a canvass position event received by the cluster node.
     *
     * @param logLeadershipTermId leadershipTermId reached by the member for it recorded log.
     * @param logPosition         position the member has durably recorded.
     * @param followerMemberId    member who sent the event.
     */
    public void logCanvassPosition(final long logLeadershipTermId, final long logPosition, final int followerMemberId)
    {
        final int length = canvassPositionLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(CANVASS_POSITION.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeCanvassPosition(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    logLeadershipTermId,
                    logPosition,
                    followerMemberId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a request to vote from a cluster candidate for leadership.
     *
     * @param logLeadershipTermId leadershipTermId processes from the log by the candidate.
     * @param logPosition         position reached in the log for the latest leadership term.
     * @param candidateTermId     the term id as the candidate sees it for the election.
     * @param candidateId         id of the candidate node.
     */
    public void logRequestVote(
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId)
    {
        final int length = requestVoteLength();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(REQUEST_VOTE.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeRequestVote(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    logLeadershipTermId,
                    logPosition,
                    candidateTermId,
                    candidateId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }
}
