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

                asyncPool.Get()->Start(ranges, *this, config.GetConnectionsLimit(), config.GetConnectionTimeout());

                EnsureConnected(config.GetConnectionTimeout());
            }

            void DataRouter::Close()
            {
                asyncPool.Get()->Close();
            }

            bool DataRouter::EnsureConnected(int32_t timeout)
            {
                //TODO: implement me.
                //throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to establish connection with any host.");
            }

            void DataRouter::OnConnectionSuccess(const network::EndPoint&, uint64_t id)
            {
                SP_DataChannel channel(new DataChannel(id, asyncPool, config, typeMgr, *this));

                {
                    common::concurrent::CsLockGuard lock(channelsMutex);

                    channels[id] = channel;
                }

                channel.Get()->StartHandshake();
            }

            void DataRouter::OnConnectionError(const network::EndPoint& addr, const IgniteError& err)
            {
                //TODO: implement me.
            }

            void DataRouter::OnMessageReceived(uint64_t id, impl::interop::SP_InteropMemory msg)
            {
                SP_DataChannel channel;

                {
                    common::concurrent::CsLockGuard lock(channelsMutex);

                    ChannelsIdMap::iterator it = channels.find(id);
                    if (it != channels.end())
                        channel = it->second;
                }

                if (channel.IsValid())
                    channel.Get()->ProcessMessage(msg);
            }

            void DataRouter::OnConnectionBroken(uint64_t id, const IgniteError& err)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                SP_DataChannel channel;

                ChannelsIdMap::iterator it = channels.find(id);
                if (it != channels.end())
                    channel = it->second;

                InvalidateChannel(channel);
            }

            void DataRouter::OnHandshakeComplete(uint32_t id)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                SP_DataChannel channel;

                ChannelsIdMap::iterator it = channels.find(id);
                if (it != channels.end())
                    channel = it->second;

                if (channel.IsValid())
                {
                    const IgniteNode& node = channel.Get()->GetNode();
                    if (!node.IsLegacy())
                        partChannels[node.GetGuid()] = channel;
                }
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

                common::concurrent::CsLockGuard lock(channelsMutex);

                DataChannel& channel0 = *channel.Get();
                channels.erase(channel0.GetId());
                partChannels.erase(channel0.GetNode().GetGuid());

                channel = SP_DataChannel();
            }

            SP_DataChannel DataRouter::GetRandomChannel()
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                return GetRandomChannelUnsafe();
            }

            SP_DataChannel DataRouter::GetRandomChannelUnsafe()
            {
                if (channels.empty())
                    return SP_DataChannel();

                int r = rand();

                size_t idx = r % channels.size();

                ChannelsIdMap::iterator it = channels.begin();

                std::advance(it, idx);

                return it->second;
            }

            SP_DataChannel DataRouter::GetBestChannel(const Guid& hint)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                ChannelsGuidMap::iterator itChannel = partChannels.find(hint);

                if (itChannel != partChannels.end())
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

