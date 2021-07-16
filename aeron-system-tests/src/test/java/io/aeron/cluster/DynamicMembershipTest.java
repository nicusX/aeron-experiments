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

import io.aeron.test.ClusterTestWatcher;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.cluster.service.Cluster.Role.FOLLOWER;
import static io.aeron.test.cluster.TestCluster.*;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith(InterruptingTestCallback.class)
public class DynamicMembershipTest
{
    private TestCluster cluster = null;

    @RegisterExtension
    final ClusterTestWatcher clusterTestWatcher = new ClusterTestWatcher();

    @AfterEach
    void tearDown()
    {
        assertEquals(
            0, clusterTestWatcher.errorCount(), "Errors observed in cluster test");
    }

    @Test
    @InterruptAfter(30)
    public void shouldQueryClusterMembers(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final ClusterMembership clusterMembership = leader.clusterMembership();

        assertEquals(leader.index(), clusterMembership.leaderMemberId);
        assertEquals("", clusterMembership.passiveMembersStr);
        assertEquals(cluster.staticClusterMembers(), clusterMembership.activeMembersStr);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshots(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        final ClusterMembership clusterMembership = awaitMembershipSize(leader, 4);

        assertEquals(leader.index(), clusterMembership.leaderMemberId);
        assertEquals("", clusterMembership.passiveMembersStr);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsThenSend(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(leader, messageCount);
        cluster.awaitServiceMessageCount(dynamicMember, messageCount);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsWithCatchup(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServiceMessageCount(leader, messageCount);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        cluster.awaitServiceMessageCount(dynamicMember, messageCount);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeWithEmptySnapshot(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshot(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember);
        assertEquals(messageCount, dynamicMember.service().messageCount());
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeWithSnapshotThenSend(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
        cluster.connectClient();
        cluster.sendMessages(preSnapshotMessageCount);
        cluster.awaitResponseMessageCount(preSnapshotMessageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);
        assertTrue(cluster.client().sendKeepAlive());

        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        assertTrue(cluster.client().sendKeepAlive());
        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        cluster.awaitSnapshotLoadedForService(dynamicMember);
        assertEquals(preSnapshotMessageCount, dynamicMember.service().messageCount());

        cluster.sendMessages(postSnapshotMessageCount);
        cluster.awaitResponseMessageCount(totalMessageCount);
        cluster.awaitServiceMessageCount(dynamicMember, totalMessageCount);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveFollower(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode follower = cluster.followers().get(0);

        follower.isTerminationExpected(true);
        leader.removeMember(follower.index(), false);

        cluster.awaitNodeTermination(follower);
        cluster.stopNode(follower);

        final ClusterMembership clusterMembership = awaitMembershipSize(leader, 2);
        assertEquals(leader.index(), clusterMembership.leaderMemberId);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveLeader(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();

        initialLeader.isTerminationExpected(true);
        initialLeader.removeMember(initialLeader.index(), false);

        cluster.awaitNodeTermination(initialLeader);
        cluster.stopNode(initialLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
        final ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 2);

        assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
        assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveLeaderAfterDynamicNodeJoined(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        awaitMembershipSize(initialLeader, 4);

        initialLeader.isTerminationExpected(true);
        initialLeader.removeMember(initialLeader.index(), false);

        cluster.awaitNodeTermination(initialLeader);
        cluster.stopNode(initialLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeader.index());
        final ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 3);

        assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
        assertNotEquals(initialLeader.index(), clusterMembership.leaderMemberId);
    }

    @Test
    @InterruptAfter(30)
    public void shouldRemoveLeaderAfterDynamicNodeJoinedThenRestartCluster(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        awaitMembershipSize(initialLeader, 4);

        final int initialLeaderIndex = initialLeader.index();
        initialLeader.isTerminationExpected(true);
        initialLeader.removeMember(initialLeaderIndex, false);

        cluster.awaitNodeTermination(initialLeader);
        cluster.stopNode(initialLeader);

        final TestNode newLeader = cluster.awaitLeader(initialLeaderIndex);
        final ClusterMembership clusterMembership = awaitMembershipSize(newLeader, 3);

        assertEquals(newLeader.index(), clusterMembership.leaderMemberId);
        assertNotEquals(initialLeaderIndex, clusterMembership.leaderMemberId);

        cluster.stopAllNodes();

        for (int i = 0; i < 3; i++)
        {
            if (initialLeaderIndex != i)
            {
                cluster.startStaticNode(i, false);
            }
        }

        cluster.awaitLeader();
        assertEquals(1, cluster.followers().size());
        awaitElectionClosed(cluster.followers().get(0));
    }

    @Test
    @InterruptAfter(30)
    public void shouldJoinDynamicNodeToSingleStaticLeader(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(1).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode initialLeader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(1, true);

        awaitElectionClosed(dynamicMember);
        awaitMembershipSize(initialLeader, 2);
    }

    @Test
    @InterruptAfter(30)
    public void shouldDynamicallyJoinClusterOfThreeNoSnapshotsAndRestartDynamicNode(final TestInfo testInfo)
    {
        cluster = aCluster().withStaticNodes(3).withDynamicNodes(1).start();
        clusterTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        final TestNode dynamicMember = cluster.startDynamicNode(3, true);

        awaitElectionClosed(dynamicMember);
        assertEquals(FOLLOWER, dynamicMember.role());

        final ClusterMembership clusterMembership = awaitMembershipSize(leader, 4);

        assertEquals(leader.index(), clusterMembership.leaderMemberId);
        assertEquals("", clusterMembership.passiveMembersStr);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(dynamicMember);
        final TestNode staticMember = cluster.startStaticNodeFromDynamicNode(3);

        awaitElectionClosed(staticMember);
        cluster.awaitServiceMessageCount(cluster.node(3), messageCount);
    }
}
