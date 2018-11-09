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

#include <cstring>
#include <cstddef>
#include <cstdlib>

#include <sstream>
#include <iterator>
#include <algorithm>

#include "impl/utility.h"
#include "impl/data_router.h"
#include "impl/message.h"
#include "impl/response_status.h"
#include "impl/ssl/ssl_gateway.h"
#include "impl/net/remote_type_updater.h"
#include "impl/net/net_utils.h"
#include "ignite/impl/thin/writable_key.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            DataRouter::DataRouter(const ignite::thin::IgniteClientConfiguration& cfg) :
                ioTimeout(DEFALT_IO_TIMEOUT),
                connectionTimeout(DEFALT_CONNECT_TIMEOUT),
                config(cfg),
                ranges(),
                localAddresses(),
                typeUpdater(),
                typeMgr()
            {
                srand(common::GetRandSeed());

                typeUpdater.reset(new net::RemoteTypeUpdater(*this));

                typeMgr.SetUpdater(typeUpdater.get());

                CollectAddresses(config.GetEndPoints(), ranges);
            }

            DataRouter::~DataRouter()
            {
                // No-op.
            }

            void DataRouter::Connect()
            {
                using ignite::thin::SslMode;

                UpdateLocalAddresses();

                channels.clear();

                if (config.GetEndPoints().empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "No valid address to connect.");

                for (std::vector<net::TcpRange>::iterator it = ranges.begin(); it != ranges.end(); ++it)
                {
                    net::TcpRange& range = *it;

                    for (uint16_t port = range.port; port <= range.port + range.range; ++port)
                    {
                        SP_DataChannel channel(new DataChannel(config, typeMgr));

                        bool connected = channel.Get()->Connect(range.host, port, connectionTimeout);

                        if (connected)
                        {
                            common::concurrent::CsLockGuard lock(channelsMutex);

                            channels[channel.Get()->GetAddress()].Swap(channel);

                            break;
                        }
                    }

                    if (!channels.empty())
                        break;
                }

                if (channels.empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to establish connection with any host.");
            }

            void DataRouter::Close()
            {
                typeMgr.SetUpdater(0);

                std::map<net::EndPoint, SP_DataChannel>::iterator it;

                common::concurrent::CsLockGuard lock(channelsMutex);

                for (it = channels.begin(); it != channels.end(); ++it)
                {
                    DataChannel* channel = it->second.Get();

                    if (channel)
                        channel->Close();
                }
            }

            void DataRouter::RefreshAffinityMapping(int32_t cacheId, bool binary)
            {
                std::vector<ConnectableNodePartitions> nodeParts;

                CacheRequest<RequestType::CACHE_NODE_PARTITIONS> req(cacheId, binary);
                ClientCacheNodePartitionsResponse rsp(nodeParts);

                SyncMessageNoMetaUpdate(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                cache::SP_CacheAffinityInfo newMapping(new cache::CacheAffinityInfo(nodeParts));

                common::concurrent::CsLockGuard lock(cacheAffinityMappingMutex);

                cache::SP_CacheAffinityInfo& affinityInfo = cacheAffinityMapping[cacheId];
                affinityInfo.Swap(newMapping);
            }

            cache::SP_CacheAffinityInfo DataRouter::GetAffinityMapping(int32_t cacheId)
            {
                common::concurrent::CsLockGuard lock(cacheAffinityMappingMutex);

                return cacheAffinityMapping[cacheId];
            }

            void DataRouter::ReleaseAffinityMapping(int32_t cacheId)
            {
                common::concurrent::CsLockGuard lock(cacheAffinityMappingMutex);

                cacheAffinityMapping.erase(cacheId);
            }

            SP_DataChannel DataRouter::GetRandomChannel()
            {
                int r = rand();

                common::concurrent::CsLockGuard lock(channelsMutex);

                size_t idx = r % channels.size();

                std::map<net::EndPoint, SP_DataChannel>::iterator it = channels.begin();

                std::advance(it, idx);

                return it->second;
            }

            bool DataRouter::IsLocalHost(const std::vector<net::EndPoint>& hint)
            {
                for (std::vector<net::EndPoint>::const_iterator it = hint.begin(); it != hint.end(); ++it)
                {
                    const std::string& host = it->host;

                    if (IsLocalAddress(host))
                        continue;

                    if (localAddresses.find(host) == localAddresses.end())
                        return false;
                }

                return true;
            }

            bool DataRouter::IsLocalAddress(const std::string& host)
            {
                static const std::string s127("127");

                bool ipv4 = std::count(host.begin(), host.end(), '.') == 3;

                if (ipv4)
                    return host.compare(0, 3, s127) == 0;

                return host == "::1" || host == "0:0:0:0:0:0:0:1";
            }

            bool DataRouter::IsProvidedByUser(const net::EndPoint& endPoint)
            {
                for (std::vector<net::TcpRange>::iterator it = ranges.begin(); it != ranges.end(); ++it)
                {
                    if (it->host == endPoint.host &&
                        endPoint.port >= it->port &&
                        endPoint.port <= it->port + it->range)
                        return true;
                }

                return false;
            }

            SP_DataChannel DataRouter::GetBestChannel(const std::vector<net::EndPoint>& hint)
            {
                if (hint.empty())
                    return GetRandomChannel();

                bool localHost = IsLocalHost(hint);

                for (std::vector<net::EndPoint>::const_iterator it = hint.begin(); it != hint.end(); ++it)
                {
                    if (IsLocalAddress(it->host) && !localHost)
                        continue;

                    if (!IsProvidedByUser(*it))
                        continue;

                    common::concurrent::CsLockGuard lock(channelsMutex);

                    SP_DataChannel& dst = channels[*it];

                    if (dst.IsValid())
                        return dst;

                    SP_DataChannel channel(new DataChannel(config, typeMgr));

                    bool connected = channel.Get()->Connect(it->host, it->port, connectionTimeout);

                    if (connected)
                    {
                        dst.Swap(channel);

                        return dst;
                    }
                }

                return GetRandomChannel();
            }

            void DataRouter::UpdateLocalAddresses()
            {
                localAddresses.clear();

                net::net_utils::GetLocalAddresses(localAddresses);
            }

            void DataRouter::CollectAddresses(const std::string& str, std::vector<net::TcpRange>& ranges)
            {
                ranges.clear();

                utility::ParseAddress(str, ranges, DEFAULT_PORT);

                std::random_shuffle(ranges.begin(), ranges.end());
            }
        }
    }
}

