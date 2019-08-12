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
#include "ignite/ignite_product_version.h"

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
                 * @param mem Memory to read Cluster Node.
                 */
                ClusterNodeImpl(common::concurrent::SharedPointer<interop::InteropMemory> mem);

                /**
                 * Destructor.
                 */
                ~ClusterNodeImpl();

                /**
                 * Get collection of addresses this node is known by.
                 *
                 * @return Collection of addresses this node is known by.
                 */
                std::vector<std::string> GetAddresses();

                /**
                 * Check if node attribute is set.
                 *
                 * @param name Node attribute name.
                 * @return True if set.
                 */
                bool IsAttributeSet(std::string name);

                /**
                 * Get a node attribute.
                 *
                 * @param name Node attribute name.
                 * @return Node attribute.
                 *
                 * @throw IgniteError in case of attribute name does not exist
                 * or if template type is not compatible with attribute.
                 */
                template<typename T>
                T GetAttribute(std::string name)
                {
                    if (attrs.Get()->find(name) == attrs.Get()->end())
                    {
                        const char* msg = "There is no Cluster Node attribute with name requested";
                        throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, msg);
                    }

                    interop::InteropInputStream stream(mem.Get());
                    stream.Position(attrs.Get()->find(name)->second);

                    binary::BinaryReaderImpl reader(&stream);

                    return reader.ReadObject<T>();
                }

                /**
                 * Get collection of all Cluster Node attributes names.
                 *
                 * @return Node attributes names collection.
                 */
                std::vector<std::string> GetAttributes();

                /**
                 * Get Cluster Node consistent ID.
                 *
                 * @return Cluster Node consistent ID.
                 */
                std::string GetConsistentId();

                /**
                 * Get collection of host names this node is known by.
                 *
                 * @return Collection of host names this node is known by.
                 */
                std::vector<std::string> GetHostNames();

                /**
                 * Gets globally unique ID.
                 *
                 * @return Cluster Node Guid.
                 */
                Guid GetId();

                /**
                 * Check if cluster node started in client mode.
                 *
                 * @return True if in client mode and false otherwise.
                 */
                bool IsClient();

                /**
                 * Check whether or not this node is a daemon.
                 *
                 * @return True if is daemon and false otherwise.
                 */
                bool IsDaemon();

                /**
                 * Check whether or not this node is a local.
                 *
                 * @return True if is local and false otherwise.
                 */
                bool IsLocal();

                /**
                 * Node order within grid topology.
                 *
                 * @return Node order.
                 */
                long GetOrder();

                /**
                 * Get node version.
                 *
                 * @return Prodcut version.
                 */
                const IgniteProductVersion& GetVersion();

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ClusterNodeImpl);

                /**
                 * Read Cluster Node addresses.
                 *
                 * @param reader Binary Reader.
                 */
                void ReadAddresses(binary::BinaryReaderImpl& reader);

                /**
                 * Read Cluster Node attributes.
                 *
                 * @param reader Binary Reader.
                 */
                void ReadAttributes(binary::BinaryReaderImpl& reader);

                /**
                 * Read Cluster Node hosts.
                 *
                 * @param reader Binary Reader.
                 */
                void ReadHosts(binary::BinaryReaderImpl& reader);

                /**
                 * Read Cluster Node consistent ID.
                 *
                 * @param reader Binary Reader.
                 */
                void ReadConsistentId(binary::BinaryReaderImpl& reader);

                /**
                 * Read Cluster Node product version.
                 *
                 * @param reader Binary Reader.
                 */
                void ReadProductVersion(binary::BinaryReaderImpl& reader);

                /** Cluster Node mem */
                common::concurrent::SharedPointer<interop::InteropMemory> mem;

                /** Addresses. */
                common::concurrent::SharedPointer<std::vector<std::string> > addrs;

                /** Attributes. */
                common::concurrent::SharedPointer<std::map<std::string, int32_t> > attrs;

                /** Hosts. */
                common::concurrent::SharedPointer<std::vector<std::string> > hosts;

                /** Node ID. */
                Guid id;

                /** Is node started in client mode. */
                bool isClient;

                /** Is node started in daemon mode. */
                bool isDaemon;

                /** Is node local. */
                bool isLocal;

                /** Order. */
                long order;

                /** Consistent ID */
                common::concurrent::SharedPointer<std::string> consistentId;

                /** Product version. */
                common::concurrent::SharedPointer<IgniteProductVersion> ver;
            };
        }
    }
}

#endif //_IGNITE_IMPL_CLUSTER_CLUSTER_NODE_IMPL