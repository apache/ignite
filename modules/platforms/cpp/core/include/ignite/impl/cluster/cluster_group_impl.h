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
                 * Get cluster group for all data nodes that have the cache with the specified name running.
                 *
                 * @param cacheName Cache name.
                 * @return Pointer to cluster group over nodes that have the cache with the specified name running.
                 */
                SP_ClusterGroupImpl ForDataNodes(std::string cacheName);

                /**
                 * Creates a cluster group of nodes started in server mode.
                 *
                 * @return Pointer to cluster group of nodes started in server mode.
                 */
                SP_ClusterGroupImpl ForServers();

                /**
                 * Creates a cluster group of cpp nodes.
                 *
                 * @return Pointer to cluster group of cpp nodes.
                 */
                SP_ClusterGroupImpl ForCpp();

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
                 * Gets the vector of nodes in this cluster group.
                 *
                 * @return All nodes in this cluster group.
                 */
                std::vector<ignite::cluster::ClusterNode> GetNodes();

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
                 * Get container of refreshed cluster nodes over this cluster group.
                 *
                 * @return Instance of compute.
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