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

#ifndef _IGNITE_IMPL_CLUSTER_CLUSTER_NODE_IMPL
#define _IGNITE_IMPL_CLUSTER_CLUSTER_NODE_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>
#include <ignite/guid.h>

#include <ignite/impl/interop/interop_target.h>

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {
            /* Forward declaration. */
            class ClusterNodeImpl;

            /* Shared pointer. */
            typedef common::concurrent::SharedPointer<ClusterNodeImpl> SP_ClusterNodeImpl;

            /**
             * Cluster node implementation.
             */
            class IGNITE_FRIEND_EXPORT ClusterNodeImpl
            {
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param reader Binary reader.
                 */
                ClusterNodeImpl(binary::BinaryReaderImpl& reader);

                /**
                 * Destructor.
                 */
                ~ClusterNodeImpl();

                /**
                 * Gets globally unique ID.
                 *
                 * @return Node Guid.
                 */
                Guid GetId();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ClusterNodeImpl);

                /** Node ID. */
                Guid id;
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_CLUSTER_NODE_IMPL