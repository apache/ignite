/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL
#define _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/impl/compute/compute_impl.h>
#include <ignite/impl/cluster/cluster_node_impl.h>

namespace ignite
{
    /* Forward declaration of interfaces. */
    namespace cluster
    {
        class ClusterGroup;
        class ClusterNode;
    }

    namespace impl
    {
        namespace cluster
        {
            /* Forward declaration. */
            class ClusterGroupImpl;

            /* Shared pointer. */
            typedef common::concurrent::SharedPointer<ClusterGroupImpl> SP_ClusterGroupImpl;

            /**
             * Cluster group implementation.
             */
            class IGNITE_FRIEND_EXPORT ClusterGroupImpl : private interop::InteropTarget
            {
                typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
                typedef common::concurrent::SharedPointer<compute::ComputeImpl> SP_ComputeImpl;
                typedef common::concurrent::SharedPointer<std::vector<ignite::cluster::ClusterNode> > SP_ClusterNodes;
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef);

                /**
                 * Destructor.
                 */
                ~ClusterGroupImpl();

                /**
                 * Get cluster group for nodes containing given name and value specified in user attributes.
                 *
                 * @param name Name of the attribute.
                 * @param val Optional attribute value to match.
                 * @return Pointer to cluster group for nodes containing specified attribute.
                 */
                SP_ClusterGroupImpl ForAttribute(std::string name, std::string val);

                /**
                 * Get cluster group for all nodes that have cache with specified name, either in client or server modes.
                 *
                 * @param cacheName Cache name.
                 * @return Pointer to cluster group over nodes that have the cache with the specified name running.
                 */
                SP_ClusterGroupImpl ForCacheNodes(std::string cacheName);

                /**
                 * Get cluster group for all client nodes that access cache with the specified name.
                 *
                 * @param cacheName Cache name.
                 * @return Pointer to cluster group over nodes that have the cache with the specified name running.
                 */
                SP_ClusterGroupImpl ForClientNodes(std::string cacheName);

                /**
                 *  Gets a cluster group consisting from the daemon nodes
                 *
                 * @return Pointer to cluster group over nodes started in daemon mode.
                 */
                SP_ClusterGroupImpl ForDaemons();

                /**
                 * Get cluster group for all data nodes that have the cache with the specified name running.
                 *
                 * @param cacheName Cache name.
                 * @return Pointer to cluster group over nodes that have the cache with the specified name running.
                 */
                SP_ClusterGroupImpl ForDataNodes(std::string cacheName);

                /**
                 * Get cluster group consisting from the nodes in this cluster group residing on the same host as the given node.
                 *
                 * @param node Cluster node.
                 * @return Pointer to cluster group residing on the same host as the given node.
                 */
                SP_ClusterGroupImpl ForHost(ignite::cluster::ClusterNode node);

                /**
                 * Get cluster group for the given node.
                 *
                 * @param node Cluster node.
                 * @return Pointer to cluster group for the given node.
                 */
                SP_ClusterGroupImpl ForNode(ignite::cluster::ClusterNode node);

                /**
                 * Get cluster group for a node with the specified ID.
                 *
                 * @param id Cluster node ID.
                 * @return Pointer to cluster group for a node with the specified ID.
                 */
                SP_ClusterGroupImpl ForNodeId(Guid id);

                /**
                 * Get cluster group over nodes with specified node IDs.
                 *
                 * @param ids Cluster node IDs.
                 * @return Pointer to cluster group over nodes with specified node IDs.
                 */
                SP_ClusterGroupImpl ForNodeIds(std::vector<Guid> ids);

                /**
                 * Get cluster group over a given set of nodes.
                 *
                 * @param nodes Cluster nodes.
                 * @return Pointer to cluster group over a given set of nodes.
                 */
                SP_ClusterGroupImpl ForNodes(std::vector<ignite::cluster::ClusterNode> nodes);

                /**
                 * Get cluster group with one oldest node from the current cluster group.
                 *
                 * @param nodes Cluster nodes.
                 * @return Pointer to cluster group with one oldest node from the current cluster group.
                 */
                SP_ClusterGroupImpl ForOldest();

                /**
                 * Get cluster group with one random node from the current cluster group.
                 *
                 * @return Pointer to cluster group with one random node from the current cluster group.
                 */
                SP_ClusterGroupImpl ForRandom();

                /**
                 * Get cluster group consisting from the nodes in this cluster group excluding the local node.
                 *
                 * @return Pointer to cluster group consisting from the nodes in this cluster group excluding the local node.
                 */
                SP_ClusterGroupImpl ForRemotes();

                /**
                 * Creates a cluster group of nodes started in server mode.
                 *
                 * @return Pointer to cluster group of nodes started in server mode.
                 */
                SP_ClusterGroupImpl ForServers();

                /**
                 * Get cluster group with one youngest node in the current cluster group.
                 *
                 * @return Pointer to cluster group with one youngest node in the current cluster group.
                 */
                SP_ClusterGroupImpl ForYoungest();

                /**
                 * Creates a cluster group of cpp nodes.
                 *
                 * @return Pointer to cluster group of cpp nodes.
                 */
                SP_ClusterGroupImpl ForCpp();

                /**
                 * Get first node from the list of nodes in this cluster group.
                 *
                 * @return Cluster node in this cluster group.
                 *
                 * @throw IgniteError if there are no nodes in the cluster group.
                 */
                ignite::cluster::ClusterNode GetNode();

                /**
                 * Get node for given ID from this cluster group.
                 *
                 * @param nid Cluster node ID.
                 * @return Cluster node in this cluster group.
                 *
                 * @throw IgniteError if there is no node with specified ID.
                 */
                ignite::cluster::ClusterNode GetNode(Guid nid);

                /**
                 * Gets the vector of nodes in this cluster group.
                 *
                 * @return All nodes in this cluster group.
                 */
                std::vector<ignite::cluster::ClusterNode> GetNodes();

                /**
                 * Get compute instance over this cluster group.
                 *
                 * @return Pointer to compute instance.
                 */
                SP_ComputeImpl GetCompute();

                /**
                 * Get compute instance over specified cluster group.
                 *
                 * @return Pointer to compute instance.
                 */
                SP_ComputeImpl GetCompute(ignite::cluster::ClusterGroup grp);

                /**
                 * Check if the Ignite grid is active.
                 *
                 * @return True if grid is active and false otherwise.
                 */
                bool IsActive();

                /**
                 * Change Ignite grid state to active or inactive.
                 *
                 * @param active If true start activation process. If false start
                 *    deactivation process.
                 */
                void SetActive(bool active);

                /**
                 * Disable write-ahead logging for specified cache.
                 *
                 * @param cacheName Cache name.
                 */
                void DisableWal(std::string cacheName);

                /**
                 * Enable write-ahead logging for specified cache.
                 *
                 * @param cacheName Cache name.
                 */
                void EnableWal(std::string cacheName);

                /**
                 * Check if write - ahead logging is enabled for specified cache.
                 *
                 * @param cacheName Cache name.
                 *
                 * @return True if enabled.
                 */
                bool IsWalEnabled(std::string cacheName);

                /**
                 * Set baseline topology constructed from the cluster topology of the given version.
                 * The method succeeds only if the cluster topology has not changed.
                 *
                 * @param topVer Topology version.
                 */
                void SetBaselineTopologyVersion(long topVer);

                /**
                 * Set transaction timeout on partition map exchange.
                 *
                 * @param timeout Timeout in milliseconds.
                 */
                void SetTxTimeoutOnPartitionMapExchange(long timeout);

                /**
                 * Ping node.
                 *
                 * @param nid Cluster node ID.
                 * @return True in case of success.
                 */
                bool PingNode(Guid nid);

                /**
                 * Get a topology by version.
                 *
                 * @param version Topology version.
                 * @return Nodes collection for the requested topology version.
                 */
                std::vector<ignite::cluster::ClusterNode> GetTopology(long version);

                /**
                 * Get current topology version.
                 *
                 * @return Current topology version.
                 */
                long GetTopologyVersion();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ClusterGroupImpl);

                /**
                 * Cluster group over nodes that have specified cache running.
                 *
                 * @param cache name to include into cluster group.
                 * @param operation id.
                 * @return Pointer to cluster group.
                 */
                SP_ClusterGroupImpl ForCacheNodes(std::string name, int32_t op);

                /**
                 * Make cluster group using java reference and
                 * internal state of this cluster group.
                 *
                 * @param javaRef Java reference to cluster group to be created.
                 * @return Pointer to cluster group.
                 */
                SP_ClusterGroupImpl FromTarget(jobject javaRef);

                /**
                 * Gets instance of compute internally.
                 *
                 * @return Pointer to compute.
                 */
                SP_ComputeImpl InternalGetCompute();

                /**
                 * Read cluster nodes from stream.
                 *
                 * @return Pointer to container of cluster nodes.
                 */
                SP_ClusterNodes ReadNodes(binary::BinaryReaderImpl& reader);

                /**
                 * Get container of refreshed cluster nodes over this cluster group.
                 *
                 * @return Vector of cluster nodes.
                 */
                std::vector<ignite::cluster::ClusterNode> RefreshNodes();

                /** Compute for the cluster group. */
                SP_ComputeImpl computeImpl;

                /** Cluster nodes. */
                SP_ClusterNodes nodes;

                /** Cluster nodes lock. */
                common::concurrent::CriticalSection nodesLock;

                /** Cluster nodes top version. */
                int64_t topVer;
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL