/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#ifndef _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL
#define _IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/impl/compute/compute_impl.h>

namespace ignite
{
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
                 * Get server nodes cluster group implementation.
                 *
                 * @return Server nodes cluster group implementation.
                 */
                SP_ClusterGroupImpl ForServers();

                /**
                 * Get compute instance over this cluster group.
                 *
                 * @return Compute instance.
                 */
                SP_ComputeImpl GetCompute();

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
                 * Make cluster group implementation using java reference and
                 * internal state of this cluster group.
                 *
                 * @param javaRef Java reference to cluster group to be created.
                 * @return New cluster group implementation.
                 */
                SP_ClusterGroupImpl FromTarget(jobject javaRef);

                /**
                 * Gets instance of compute internally.
                 *
                 * @return Instance of compute.
                 */
                SP_ComputeImpl InternalGetCompute();

                /** Compute for the cluster group. */
                SP_ComputeImpl computeImpl;
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_CLUSTER_GROUP_IMPL