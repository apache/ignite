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

#include <ignite/jni/java.h>

#include <ignite/impl/cluster/cluster_node_impl.h>

using namespace ignite::jni::java;
using namespace ignite::common::concurrent;
using namespace ignite::impl::cluster;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {
            ClusterNodeImpl::ClusterNodeImpl(SharedPointer<InteropMemory> mem) :
                mem(mem), addrs(new std::vector<std::string>), attrs(new std::map<std::string, int32_t>), hosts(new std::vector<std::string>),
                isClient(false), isDaemon(false), isLocal(false), consistentId(new std::string)
            {
                InteropInputStream stream(mem.Get());
                BinaryReaderImpl reader(&stream);

                id = reader.ReadGuid();

                ReadAttributes(reader);
                ReadAddresses(reader);
                ReadHosts(reader);

                order = reader.ReadInt64();
                isLocal = reader.ReadBool();
                isDaemon = reader.ReadBool();
                isClient = reader.ReadBool();

                ReadConsistentId(reader);
                ReadProductVersion(reader);
            }

            ClusterNodeImpl::~ClusterNodeImpl()
            {
                // No-op.
            }

            const std::vector<std::string>& ClusterNodeImpl::GetAddresses() const
            {
                return *addrs.Get();
            }

            bool ClusterNodeImpl::IsAttributeSet(std::string name) const
            {
                return attrs.Get()->find(name) != attrs.Get()->end() ? true : false;
            }

            std::vector<std::string> ClusterNodeImpl::GetAttributes() const
            {
                std::vector<std::string> ret;

                for (std::map<std::string, int32_t>::const_iterator it = attrs.Get()->begin();
                    it != attrs.Get()->end(); ++it)
                    ret.push_back(it->first);

                return ret;
            }

            std::string ClusterNodeImpl::GetConsistentId() const
            {
                return *consistentId.Get();
            }

            const std::vector<std::string>& ClusterNodeImpl::GetHostNames() const
            {
                return *hosts.Get();
            }

            Guid ClusterNodeImpl::GetId() const
            {
                return id;
            }

            bool ClusterNodeImpl::IsClient() const
            {
                return isClient;
            }

            bool ClusterNodeImpl::IsDaemon() const
            {
                return isDaemon;
            }

            bool ClusterNodeImpl::IsLocal() const
            {
                return isLocal;
            }

            int64_t ClusterNodeImpl::GetOrder() const
            {
                return order;
            }

            const IgniteProductVersion& ClusterNodeImpl::GetVersion() const
            {
                return *ver.Get();
            }

            void ClusterNodeImpl::ReadAddresses(BinaryReaderImpl& reader)
            {
                std::back_insert_iterator<std::vector<std::string> > iter(*addrs.Get());

                reader.ReadCollection<std::string>(iter);
            }

            void ClusterNodeImpl::ReadAttributes(BinaryReaderImpl& reader)
            {
                int32_t cnt = reader.ReadInt32();
                for (int32_t i = 0; i < cnt; i++)
                {
                    std::string name = reader.ReadObject<std::string>();
                    int32_t pos = reader.GetStream()->Position();
                    attrs.Get()->insert(std::pair<std::string, int32_t>(name, pos));
                    reader.Skip();
                }
            }

            void ClusterNodeImpl::ReadHosts(BinaryReaderImpl& reader)
            {
                std::back_insert_iterator<std::vector<std::string> > iter(*hosts.Get());

                reader.ReadCollection<std::string>(iter);
            }

            void ClusterNodeImpl::ReadConsistentId(BinaryReaderImpl& reader)
            {
                int8_t typeId = reader.ReadInt8();
                reader.GetStream()->Position(reader.GetStream()->Position() - 1);
                if (typeId == IGNITE_TYPE_STRING)
                {
                    reader.ReadString(*consistentId.Get());
                    return;
                }

                std::stringstream ss;
                ss << reader.ReadGuid();
                *consistentId.Get() = ss.str();
            }

            void ClusterNodeImpl::ReadProductVersion(BinaryReaderImpl& reader)
            {
                int8_t major = reader.ReadInt8();
                int8_t minor = reader.ReadInt8();
                int8_t maintenance = reader.ReadInt8();

                std::string stage;
                reader.ReadString(stage);

                int64_t releaseDate = reader.ReadInt64();

                std::vector<int8_t> revHash(IgniteProductVersion::SHA1_LENGTH);
                reader.ReadInt8Array(&revHash[0], IgniteProductVersion::SHA1_LENGTH);

                ver = SharedPointer<IgniteProductVersion>(new IgniteProductVersion(major, minor,
                    maintenance, stage, releaseDate, revHash));
            }
        }
    }
}
