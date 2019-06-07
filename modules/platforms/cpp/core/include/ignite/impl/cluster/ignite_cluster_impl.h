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

#ifndef _IGNITE_IMPL_CLUSTER_IGNITE_CLUSTER_IMPL
#define _IGNITE_IMPL_CLUSTER_IGNITE_CLUSTER_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/impl/cluster/cluster_group_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {
            /**
             * Ignite cluster implementation.
             */
            class IGNITE_FRIEND_EXPORT IgniteClusterImpl
            {
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param impl Pointer to ClusterGroupImpl.
                 */
                IgniteClusterImpl(SP_ClusterGroupImpl impl);

                /**
                 * Destructor.
                 */
                ~IgniteClusterImpl();

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
                 * @return ClusterGroupImpl instance.
                 */
                SP_ClusterGroupImpl AsClusterGroup();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(IgniteClusterImpl);

                /** Implementation. */
                SP_ClusterGroupImpl impl;
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_IGNITE_CLUSTER_IMPL