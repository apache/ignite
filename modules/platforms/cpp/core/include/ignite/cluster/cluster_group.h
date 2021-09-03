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
  * Declares ignite::cluster::ClusterGroup class.
  */

#ifndef _IGNITE_CLUSTER_CLUSTER_GROUP
#define _IGNITE_CLUSTER_CLUSTER_GROUP

#include <ignite/cluster/cluster_node.h>

#include <ignite/impl/cluster/cluster_group_impl.h>

namespace ignite
{
    namespace impl
    {
        class IgniteImpl;
    }

    namespace cluster
    {
        /**
         * Defines a cluster group which contains all or a subset of cluster nodes.
         * Cluster group allows to group cluster nodes into various subgroups to perform distributed operations on them.
         * The IgniteCluster interface itself also contains the ClusterGroup which makes an instance of IgniteCluster
         * into a cluster group containing all cluster nodes. Use IgniteCluster::AsClusterGroup() to get the cluster group in this case.
         */
        class IGNITE_IMPORT_EXPORT ClusterGroup
        {
            friend class impl::cluster::ClusterGroupImpl;
            friend class impl::IgniteImpl;
        public:
            /**
             * Constructor.
             *
             * @param pointer to cluster group implementation.
             */
            ClusterGroup(impl::cluster::SP_ClusterGroupImpl impl);

            /**
             * Get cluster group for nodes containing given name and value specified in user attributes.
             *
             * @param name Name of the attribute.
             * @param val Optional attribute value to match.
             * @return Cluster group for nodes containing specified attribute.
             */
            ClusterGroup ForAttribute(std::string name, std::string val);

            /**
             * Get cluster group for all nodes that have cache with specified name, either in client or server modes.
             *
             * @param cacheName Cache name.
             * @return Cluster group over nodes that have the cache with the specified name running.
             */
            ClusterGroup ForCacheNodes(std::string cacheName);

            /**
             * Get cluster group for all client nodes that access cache with the specified name.
             *
             * @param cacheName Cache name.
             * @return Cluster group over nodes that have the cache with the specified name running.
             */
            ClusterGroup ForClientNodes(std::string cacheName);

            /**
             * Get a cluster group of nodes started in client mode.
             *
             * @return Cluster group over nodes that started in client mode.
             */
            ClusterGroup ForClients();

            /**
             * Get cluster group consisting from the daemon nodes.
             *
             * @return Cluster group consisting from the daemon nodes.
             */
            ClusterGroup ForDaemons();

            /**
             * Get ClusterGroup for all data nodes that have the cache with the specified name running.
             *
             * @param cacheName Cache name.
             * @return Cluster group over nodes that have the cache with the specified name running.
             */
            ClusterGroup ForDataNodes(std::string cacheName);

            /**
             * Get cluster group consisting from the nodes in this cluster group residing on the same host as the given node.
             *
             * @param node Cluster node.
             * @return Cluster group residing on the same host as the given node.
             */
            ClusterGroup ForHost(ClusterNode node);

            /**
             * Get cluster group consisting from the nodes running on the host specified.
             *
             * @param hostName Host name.
             * @return Cluster group over nodes that have requested host name.
             */
            ClusterGroup ForHost(std::string hostName);

            /**
             * Get cluster group consisting from the nodes running on the hosts specified.
             *
             * @param hostNames Container of host names.
             * @return Cluster group over nodes that have requested host names.
             */
            ClusterGroup ForHosts(std::vector<std::string> hostNames);

            /**
             * Get cluster group for the given node.
             *
             * @param node Cluster node.
             * @return Cluster group for the given node.
             */
            ClusterGroup ForNode(ClusterNode node);

            /**
             * Get cluster group for a node with the specified ID.
             *
             * @param id Cluster node ID.
             * @return Cluster group for a node with the specified ID.
             */
            ClusterGroup ForNodeId(Guid id);

            /**
             * Get cluster group over nodes with specified node IDs.
             *
             * @param ids Cluster node IDs.
             * @return Cluster group over nodes with specified node IDs.
             */
            ClusterGroup ForNodeIds(std::vector<Guid> ids);

            /**
             * Get cluster group over a given set of nodes.
             *
             * @param nodes Cluster nodes.
             * @return Cluster group over a given set of nodes.
             */
            ClusterGroup ForNodes(std::vector<ClusterNode> nodes);

            /**
             * Get cluster group with one oldest node from the current cluster group.
             *
             * @return Cluster group with one oldest node from the current cluster group.
             */
            ClusterGroup ForOldest();

            /**
             * Create a new cluster group which includes all nodes that pass the given predicate filter.
             *
             * @param pred Pointer to predicate heap object. User should NOT free the memory used by object.
             * @return Newly created cluster group.
             *
             * @throw IgniteError if there are no nodes in the cluster group.
             */
            ClusterGroup ForPredicate(IgnitePredicate<ClusterNode>* pred);

            /**
             * Get cluster group with one random node from the current cluster group.
             *
             * @return Cluster group with one random node from the current cluster group.
             */
            ClusterGroup ForRandom();

            /**
             * Get cluster group consisting from the nodes in this cluster group excluding the local node.
             *
             * @return Cluster group consisting from the nodes in this cluster group excluding the local node.
             */
            ClusterGroup ForRemotes();

            /**
             * Creates a cluster group of nodes started in server mode.
             *
             * @return Cluster group of nodes started in server mode.
             */
            ClusterGroup ForServers();

            /**
             * Get cluster group with one youngest node in the current cluster group.
             *
             * @return Cluster group with one youngest node in the current cluster group.
             */
            ClusterGroup ForYoungest();

            /**
             * Creates a cluster group of cpp nodes.
             *
             * @return Cluster group of cpp nodes.
             */
            ClusterGroup ForCpp();

            /**
             * Get first node from the list of nodes in this cluster group.
             *
             * @return Cluster node in this cluster group.
             *
             * @throw IgniteError if there are no nodes in the cluster group.
             */
            ClusterNode GetNode();

            /**
             * Get node for given ID from this cluster group.
             *
             * @param nid Cluster node ID.
             * @return Cluster node in this cluster group.
             *
             * @throw IgniteError if there is no node with specified ID.
             */
            ClusterNode GetNode(Guid nid);

            /**
             * Get the vector of nodes in this cluster group.
             *
             * @return All nodes in this cluster group.
             */
            std::vector<ClusterNode> GetNodes();

            /**
             * Get predicate that defines a subset of nodes for this cluster group.
             *
             * @return Pointer to predicate.
             */
            IgnitePredicate<ClusterNode>* GetPredicate();

        private:
            impl::cluster::SP_ClusterGroupImpl impl;
            impl::cluster::SP_ClusterGroupImpl GetImpl();
        };
    }
}

#endif //_IGNITE_CLUSTER_CLUSTER_GROUP