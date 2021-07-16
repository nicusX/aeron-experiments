Aeron Cluster
===

Aeron provides support for fault-tolerant services as replicated state machines based on the 
[Raft](https://raft.github.io/) consensus algorithm.

The purpose of Aeron Cluster is to aggregate and sequence streams from cluster clients into a single log. A number of
nodes will replicate and archive the log to achieve fault tolerance. Cluster services process the log and respond
to cluster clients.

Aeron Cluster works on the concept of a strong leader. The leader sequences the log and is responsible for replicating
the log to other cluster members known as followers.

A number of components make up Aeron Cluster. Central is the Consensus Module which sequences the log and
coordinates consensus for the recording of the sequenced log to persistent storage, and the services consuming the log
across cluster members. Aeron Archive records the log to durable storage. Services consume the log once a majority of
the cluster members have safely recorded the log to durable storage.

To enable fast recovery, the services and consensus module can take a snapshots of their state as of a given log
position. Snapshots enable recovery by loading the most recent snapshot and replaying logs from that point forward.
The Archive records snapshots for local, and remote, replay thus avoiding the need for a distributed
file system.

Unique features to Aeron Cluster include support for reliable distributed timers, inter-service messaging, remote data
centre backup, and unparalleled performance.

[Cluster Tutorial](https://github.com/real-logic/aeron/wiki/Cluster-Tutorial) is a good place to start.

Usage
=====

The cluster can run in various configurations:

 - **Single Node:** For development, debugging, or when a sequenced and archived log on a single node is sufficient.
 - **Appointed Leader:** A leader of the cluster can be appointed via configuration without requiring an election.
    In the event of a leader failure then a follower will have to be manually appointed the new leader.
 - **Automatic Elections:** Automatic elections (default) can be enabled to have a leader elected at random from the
    members with the most up to date log.
 - **Dynamic Membership:** Cluster node membership can be dynamic with support for members to join and quit the cluster
    with membership changes recorded in the log.
       
The majority of cluster members determine consensus. Clusters should be 3 or 5 in population size. However, 2 node
clusters are supported whereby both members must agree the log and in the event of failure the remaining member must be
manually reconfigured as a single node cluster to progress.

Aeron Cluster Protocol
=====

Messages specification is in SBE in [aeron-cluster-codecs.xml](https://github.com/real-logic/aeron/blob/master/aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml).
