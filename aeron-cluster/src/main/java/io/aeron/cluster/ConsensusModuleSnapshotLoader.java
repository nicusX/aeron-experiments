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
import io.aeron.cluster.service.ClusterClock;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_TYPE_ID;

@SuppressWarnings("MethodLength")
class ConsensusModuleSnapshotLoader implements ControlledFragmentHandler
{
    static final int FRAGMENT_LIMIT = 10;

    private boolean inSnapshot = false;
    private boolean isDone = false;
    private int appVersion;
    private TimeUnit timeUnit;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
    private final ClusterSessionDecoder clusterSessionDecoder = new ClusterSessionDecoder();
    private final TimerDecoder timerDecoder = new TimerDecoder();
    private final ConsensusModuleDecoder consensusModuleDecoder = new ConsensusModuleDecoder();
    private final ClusterMembersDecoder clusterMembersDecoder = new ClusterMembersDecoder();
    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(this);
    private final Image image;
    private final ConsensusModuleAgent consensusModuleAgent;

    ConsensusModuleSnapshotLoader(final Image image, final ConsensusModuleAgent agent)
    {
        this.image = image;
        this.consensusModuleAgent = agent;
    }

    boolean isDone()
    {
        return isDone;
    }

    public int appVersion()
    {
        return appVersion;
    }

    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    int poll()
    {
        return image.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                consensusModuleAgent.onLoadPendingMessage(buffer, offset, length);
                break;

            case SnapshotMarkerDecoder.TEMPLATE_ID:
                snapshotMarkerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long typeId = snapshotMarkerDecoder.typeId();
                if (typeId != SNAPSHOT_TYPE_ID)
                {
                    throw new ClusterException("unexpected snapshot type: " + typeId);
                }

                switch (snapshotMarkerDecoder.mark())
                {
                    case BEGIN:
                        if (inSnapshot)
                        {
                            throw new ClusterException("already in snapshot");
                        }
                        inSnapshot = true;
                        appVersion = snapshotMarkerDecoder.appVersion();
                        timeUnit = ClusterClock.map(snapshotMarkerDecoder.timeUnit());
                        return Action.CONTINUE;

                    case END:
                        if (!inSnapshot)
                        {
                            throw new ClusterException("missing begin snapshot");
                        }
                        isDone = true;
                        return Action.BREAK;
                }
                break;

            case ClusterSessionDecoder.TEMPLATE_ID:
                clusterSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onLoadSession(
                    clusterSessionDecoder.clusterSessionId(),
                    clusterSessionDecoder.correlationId(),
                    clusterSessionDecoder.openedLogPosition(),
                    clusterSessionDecoder.timeOfLastActivity(),
                    clusterSessionDecoder.closeReason(),
                    clusterSessionDecoder.responseStreamId(),
                    clusterSessionDecoder.responseChannel());
                break;

            case TimerDecoder.TEMPLATE_ID:
                timerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onScheduleTimer(timerDecoder.correlationId(), timerDecoder.deadline());
                break;

            case ConsensusModuleDecoder.TEMPLATE_ID:
                consensusModuleDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onLoadConsensusModuleState(
                    consensusModuleDecoder.nextSessionId(),
                    consensusModuleDecoder.nextServiceSessionId(),
                    consensusModuleDecoder.logServiceSessionId(),
                    consensusModuleDecoder.pendingMessageCapacity());
                break;

            case ClusterMembersDecoder.TEMPLATE_ID:
                clusterMembersDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onLoadClusterMembers(
                    clusterMembersDecoder.memberId(),
                    clusterMembersDecoder.highMemberId(),
                    clusterMembersDecoder.clusterMembers());
                break;
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }
}
