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

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

import java.util.ArrayList;
import java.util.List;

final class ClusterControlAdapter implements AutoCloseable
{
    interface Listener
    {
        void onClusterMembersResponse(
            long correlationId, int leaderMemberId, String activeMembers, String passiveMembers);

        void onClusterMembersExtendedResponse(
            long correlationId,
            long currentTimeNs,
            int leaderMemberId,
            int memberId,
            List<ClusterMember> activeMembers,
            List<ClusterMember> passiveMembers);
    }

    private final Subscription subscription;
    private final Listener listener;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ClusterMembersResponseDecoder clusterMembersResponseDecoder = new ClusterMembersResponseDecoder();
    private final ClusterMembersExtendedResponseDecoder clusterMembersExtendedResponseDecoder =
        new ClusterMembersExtendedResponseDecoder();

    ClusterControlAdapter(final Subscription subscription, final Listener listener)
    {
        this.subscription = subscription;
        this.listener = listener;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    int poll()
    {
        return subscription.poll(fragmentAssembler, 1);
    }

    @SuppressWarnings("MethodLength")
    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        if (templateId == ClusterMembersResponseDecoder.TEMPLATE_ID)
        {
            clusterMembersResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            listener.onClusterMembersResponse(
                clusterMembersResponseDecoder.correlationId(),
                clusterMembersResponseDecoder.leaderMemberId(),
                clusterMembersResponseDecoder.activeMembers(),
                clusterMembersResponseDecoder.passiveFollowers());
        }
        else if (templateId == ClusterMembersExtendedResponseDecoder.TEMPLATE_ID)
        {
            clusterMembersExtendedResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long correlationId = clusterMembersExtendedResponseDecoder.correlationId();
            final long currentTimeNs = clusterMembersExtendedResponseDecoder.currentTimeNs();
            final int leaderMemberId = clusterMembersExtendedResponseDecoder.leaderMemberId();
            final int memberId = clusterMembersExtendedResponseDecoder.memberId();

            final ArrayList<ClusterMember> activeMembers = new ArrayList<>();
            for (final ClusterMembersExtendedResponseDecoder.ActiveMembersDecoder activeMembersDecoder :
                clusterMembersExtendedResponseDecoder.activeMembers())
            {
                final int id = activeMembersDecoder.memberId();
                final String ingressEndpoint = activeMembersDecoder.ingressEndpoint();
                final String consensusEndpoint = activeMembersDecoder.consensusEndpoint();
                final String logEndpoint = activeMembersDecoder.logEndpoint();
                final String catchupEndpoint = activeMembersDecoder.catchupEndpoint();
                final String archiveEndpoint = activeMembersDecoder.archiveEndpoint();
                final String endpoints = String.join(
                    ",",
                    ingressEndpoint,
                    consensusEndpoint,
                    logEndpoint,
                    catchupEndpoint,
                    archiveEndpoint);

                activeMembers.add(new ClusterMember(
                    id,
                    ingressEndpoint,
                    consensusEndpoint,
                    logEndpoint,
                    catchupEndpoint,
                    archiveEndpoint,
                    endpoints)
                    .leadershipTermId(activeMembersDecoder.leadershipTermId())
                    .logPosition(activeMembersDecoder.logPosition())
                    .timeOfLastAppendPositionNs(activeMembersDecoder.timeOfLastAppendNs()));
            }

            final ArrayList<ClusterMember> passiveMembers = new ArrayList<>();
            for (final ClusterMembersExtendedResponseDecoder.PassiveMembersDecoder passiveMembersDecoder :
                clusterMembersExtendedResponseDecoder.passiveMembers())
            {
                final int id = passiveMembersDecoder.memberId();
                final String ingressEndpoint = passiveMembersDecoder.ingressEndpoint();
                final String consensusEndpoint = passiveMembersDecoder.consensusEndpoint();
                final String logEndpoint = passiveMembersDecoder.logEndpoint();
                final String catchupEndpoint = passiveMembersDecoder.catchupEndpoint();
                final String archiveEndpoint = passiveMembersDecoder.archiveEndpoint();
                final String endpoints = String.join(
                    ",",
                    ingressEndpoint,
                    consensusEndpoint,
                    logEndpoint,
                    catchupEndpoint,
                    archiveEndpoint);

                passiveMembers.add(new ClusterMember(
                    id,
                    ingressEndpoint,
                    consensusEndpoint,
                    logEndpoint,
                    catchupEndpoint,
                    archiveEndpoint,
                    endpoints)
                    .leadershipTermId(passiveMembersDecoder.leadershipTermId())
                    .logPosition(passiveMembersDecoder.logPosition())
                    .timeOfLastAppendPositionNs(passiveMembersDecoder.timeOfLastAppendNs()));
            }

            listener.onClusterMembersExtendedResponse(
                correlationId,
                currentTimeNs,
                leaderMemberId,
                memberId,
                activeMembers,
                passiveMembers);
        }
    }
}
