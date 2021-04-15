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

#include <ignite/network/utils.h>

#include "impl/utility.h"
#include "impl/data_router.h"
#include "impl/message.h"
#include "impl/response_status.h"
#include "impl/remote_type_updater.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            DataRouter::DataRouter(const ignite::thin::IgniteClientConfiguration& cfg) :
                ioTimeout(DEFAULT_IO_TIMEOUT),
                connectionTimeout(DEFAULT_CONNECT_TIMEOUT),
                config(cfg)
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

                if (ranges.empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "No valid address to connect.");

                ChannelsVector newLegacyChannels;
                newLegacyChannels.reserve(ranges.size());

                for (std::vector<network::TcpRange>::iterator it = ranges.begin(); it != ranges.end(); ++it)
                {
                    network::TcpRange& range = *it;

                    for (uint16_t port = range.port; port <= range.port + range.range; ++port)
                    {
                        SP_DataChannel channel(new DataChannel(config, typeMgr));

                        bool connected = false;

                        try
                        {
                            connected = channel.Get()->Connect(range.host, port, connectionTimeout);
                        }
                        catch (const IgniteError&)
                        {
                            // No-op.
                        }

                        if (connected)
                        {
                            const IgniteNode& newNode = channel.Get()->GetNode();

                            if (newNode.IsLegacy())
                            {
                                newLegacyChannels.push_back(channel);
                            }
                            else
                            {
                                common::concurrent::CsLockGuard lock(channelsMutex);

                                // Insertion takes place if no channel with the GUID is already present.
                                std::pair<ChannelsGuidMap::iterator, bool> res =
                                    channels.insert(std::make_pair(newNode.GetGuid(), channel));

                                bool inserted = res.second;
                                SP_DataChannel& oldChannel = res.first->second;

                                if (!inserted && !oldChannel.Get()->IsConnected())
                                    oldChannel.Swap(channel);
                            }

                            break;
                        }
                    }

                    if (config.GetConnectionsLimit())
                    {
                        common::concurrent::CsLockGuard lock(channelsMutex);

                        size_t connectionsNum = newLegacyChannels.size() + channels.size();

                        if (connectionsNum >= config.GetConnectionsLimit())
                            break;
                    }
                }

                common::concurrent::CsLockGuard lock(channelsMutex);

                legacyChannels.swap(newLegacyChannels);

                if (channels.empty() && legacyChannels.empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to establish connection with any host.");
            }

            void DataRouter::Close()
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                channels.clear();
                legacyChannels.clear();
            }

            void DataRouter::ProcessMeta(int32_t metaVer)
            {
                if (typeMgr.IsUpdatedSince(metaVer))
                {
                    IgniteError err;

                    if (!typeMgr.ProcessPendingUpdates(err))
                        throw IgniteError(err);
                }
            }

            void DataRouter::RefreshAffinityMapping(int32_t cacheId)
            {
                std::vector<int32_t> ids(1, cacheId);

                RefreshAffinityMapping(ids);
            }

            void DataRouter::RefreshAffinityMapping(const std::vector<int32_t>& cacheIds)
            {
                std::vector<PartitionAwarenessGroup> groups;

                CachePartitionsRequest req(cacheIds);
                CachePartitionsResponse rsp(groups);

                SyncMessageNoMetaUpdate(req, rsp);

                if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                    throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());

                affinityManager.UpdateAffinity(rsp.GetGroups(), rsp.GetVersion());
            }

            affinity::SP_AffinityAssignment DataRouter::GetAffinityAssignment(int32_t cacheId) const
            {
                return affinityManager.GetAffinityAssignment(cacheId);
            }

            void DataRouter::InvalidateChannel(SP_DataChannel &channel)
            {
                if (!channel.IsValid())
                    return;

                const IgniteNode& node = channel.Get()->GetNode();

                common::concurrent::CsLockGuard lock(channelsMutex);

                if (!node.IsLegacy())
                {
                    channels.erase(node.GetGuid());
                }
                else
                {
                    const network::EndPoint& ep1 = node.GetEndPoint();
                    for (ChannelsVector::iterator it = legacyChannels.begin(); it != legacyChannels.end(); ++it)
                    {
                        const network::EndPoint& ep2 = it->Get()->GetNode().GetEndPoint();

                        if (ep1 == ep2)
                        {
                            legacyChannels.erase(it);

                            break;
                        }
                    }
                }

                channel = SP_DataChannel();
            }

            SP_DataChannel DataRouter::GetRandomChannel()
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                return GetRandomChannelUnsafe();
            }

            SP_DataChannel DataRouter::GetRandomChannelUnsafe()
            {
                if (channels.empty() && legacyChannels.empty())
                    return SP_DataChannel();

                int r = rand();

                size_t idx = r % (channels.size() + legacyChannels.size());

                if (idx >= channels.size())
                {
                    size_t legacyIdx = idx - channels.size();

                    return legacyChannels[legacyIdx];
                }

                ChannelsGuidMap::iterator it = channels.begin();

                std::advance(it, idx);

                return it->second;
            }

            bool DataRouter::IsProvidedByUser(const network::EndPoint& endPoint)
            {
                for (std::vector<network::TcpRange>::iterator it = ranges.begin(); it != ranges.end(); ++it)
                {
                    if (it->host == endPoint.host &&
                        endPoint.port >= it->port &&
                        endPoint.port <= it->port + it->range)
                        return true;
                }

                return false;
            }

            SP_DataChannel DataRouter::GetBestChannel(const Guid& hint)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                ChannelsGuidMap::iterator itChannel = channels.find(hint);

                if (itChannel != channels.end())
                    return itChannel->second;

                return GetRandomChannelUnsafe();
            }

            void DataRouter::CollectAddresses(const std::string& str, std::vector<network::TcpRange>& ranges)
            {
                ranges.clear();

                utility::ParseAddress(str, ranges, DEFAULT_PORT);

                std::random_shuffle(ranges.begin(), ranges.end());
            }
        }
    }
}

