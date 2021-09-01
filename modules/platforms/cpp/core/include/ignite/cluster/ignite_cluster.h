/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /**
  * @file
  * Declares ignite::cluster::IgniteCluster class.
  */

#ifndef _IGNITE_CLUSTER_IGNITE_CLUSTER
#define _IGNITE_CLUSTER_IGNITE_CLUSTER

#include <ignite/cluster/cluster_group.h>

#include <ignite/impl/cluster/ignite_cluster_impl.h>

namespace ignite
{
    namespace cluster
    {
        /**
         * Represents whole cluster (all available nodes). Node-local map is useful for saving shared state
         * between job executions on the grid. Additionally you can also ping, start, and restart remote nodes, map keys to
         * caching nodes, and get other useful information about topology.
         */
        class IGNITE_IMPORT_EXPORT IgniteCluster
        {
        public:
            /**
             * Constructor.
             *
             * @param impl Pointer to ignite cluster implementation.
             */
            IgniteCluster(common::concurrent::SharedPointer<ignite::impl::cluster::IgniteClusterImpl> impl);

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
             * Get a cluster group consisting from the local node.
             *
             * @return Cluster group consisting from the local node.
             */
            cluster::ClusterGroup ForLocal();

            /**
             * Get local grid node.
             *
             * @return Local node.
             */
            cluster::ClusterNode GetLocalNode();

            /**
             * Set baseline topology constructed from the cluster topology of the given version.
             * The method succeeds only if the cluster topology has not changed.
             *
             * @param topVer Topology version.
             */
            void SetBaselineTopologyVersion(int64_t topVer);

            /**
             * Set transaction timeout on partition map exchange.
             *
             * @param timeout Timeout in milliseconds.
             */
            void SetTxTimeoutOnPartitionMapExchange(int64_t timeout);

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
            std::vector<ClusterNode> GetTopology(int64_t version);

            /**
             * Get current topology version.
             *
             * @return Current topology version.
             */
            int64_t GetTopologyVersion();

            /**
             * Get cluster group consisting of all cluster nodes.
             *
             * @return ClusterGroup instance.
             */
            ClusterGroup AsClusterGroup();

        private:
            common::concurrent::SharedPointer<ignite::impl::cluster::IgniteClusterImpl> impl;
        };
    }
}

#endif //_IGNITE_CLUSTER_IGNITE_CLUSTER