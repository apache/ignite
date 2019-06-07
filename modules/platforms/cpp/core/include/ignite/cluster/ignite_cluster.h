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
             * Gets cluster group consisting of all cluster nodes.
             *
             * @return ClusterGroup instance.
             */
            cluster::ClusterGroup AsClusterGroup();

        private:
            common::concurrent::SharedPointer<ignite::impl::cluster::IgniteClusterImpl> impl;
        };
    }
}

#endif //_IGNITE_CLUSTER_IGNITE_CLUSTER