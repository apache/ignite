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

#include <ignite/network/codec_data_filter.h>
#include <ignite/network/length_prefix_codec.h>
#include <ignite/network/network.h>
#include <ignite/network/utils.h>
#include <ignite/network/ssl/secure_data_filter.h>

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
                config(cfg),
                userThreadPool(cfg.GetUserThreadPoolSize())
            {
                srand(common::GetRandSeed());

                typeUpdater.reset(new net::RemoteTypeUpdater(*this));

                typeMgr.SetUpdater(typeUpdater.get());

                CollectAddresses(config.GetEndPoints(), ranges);
            }

            DataRouter::~DataRouter()
            {
                Close();
            }

            void DataRouter::Connect()
            {
                using ignite::thin::SslMode;

                if (ranges.empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "No valid address to connect.");

                if (!asyncPool.IsValid())
                {
                    std::vector<network::SP_DataFilter> filters;

                    if (config.GetSslMode() == SslMode::REQUIRE)
                    {
                        network::ssl::EnsureSslLoaded();

                        network::ssl::SecureConfiguration sslCfg;
                        sslCfg.caPath = config.GetSslCaFile();
                        sslCfg.keyPath = config.GetSslKeyFile();
                        sslCfg.certPath = config.GetSslCertFile();

                        network::ssl::SP_SecureDataFilter secureFilter(new network::ssl::SecureDataFilter(sslCfg));
                        filters.push_back(secureFilter);
                    }

                    network::SP_CodecFactory codecFactory(new network::LengthPrefixCodecFactory());
                    network::SP_CodecDataFilter codecFilter(new network::CodecDataFilter(codecFactory));
                    filters.push_back(codecFilter);

                    asyncPool = network::MakeAsyncClientPool(filters);

                    if (!asyncPool.IsValid())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not create async connection pool");

                    asyncPool.Get()->SetHandler(this);
                }

                userThreadPool.Start();
                asyncPool.Get()->Start(ranges, config.GetConnectionsLimit());

                bool connected = EnsureConnected(config.GetConnectionTimeout());

                if (!connected)
                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                        "Failed to establish connection with any host.");
            }

            void DataRouter::Close()
            {
                if (asyncPool.IsValid())
                {
                    asyncPool.Get()->SetHandler(0);
                    asyncPool.Get()->Stop();
                }

                userThreadPool.Stop();
            }

            bool DataRouter::EnsureConnected(int32_t timeout)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                if (!connectedChannels.empty())
                    return true;

                CheckHandshakeErrorLocked();

                channelsWaitPoint.WaitFor(channelsMutex, timeout);

                CheckHandshakeErrorLocked();

                return !connectedChannels.empty();
            }

            void DataRouter::CheckHandshakeErrorLocked()
            {
                if (!lastHandshakeError.get())
                    return;

                IgniteError err = *lastHandshakeError;
                lastHandshakeError.reset();

                throw IgniteError(err);
            }

            void DataRouter::OnConnectionSuccess(const network::EndPoint& addr, uint64_t id)
            {
                SP_DataChannel channel(new DataChannel(id, addr, asyncPool, config, typeMgr, *this, userThreadPool));

                {
                    common::concurrent::CsLockGuard lock(channelsMutex);

                    channels[id] = channel;
                }

                channel.Get()->StartHandshake();
            }

            void DataRouter::OnConnectionError(const network::EndPoint& addr, const IgniteError& err)
            {
                IGNITE_UNUSED(addr);

                if (!connectedChannels.empty())
                    return;

                if (err.GetCode() != IgniteError::IGNITE_ERR_SECURE_CONNECTION_FAILURE)
                    return;

                common::concurrent::CsLockGuard lock(channelsMutex);

                lastHandshakeError.reset(new IgniteError(err));
                channelsWaitPoint.NotifyAll();
            }

            void DataRouter::OnConnectionClosed(uint64_t id, const IgniteError* err)
            {
                SP_DataChannel channel;
                {
                    common::concurrent::CsLockGuard lock(channelsMutex);

                    channel = FindChannelLocked(id);

                    connectedChannels.erase(id);
                    InvalidateChannelLocked(channel);
                }

                if (channel.IsValid())
                    channel.Get()->FailPendingRequests(err);
            }

            void DataRouter::OnMessageReceived(uint64_t id, const network::DataBuffer& msg)
            {
                SP_DataChannel channel = FindChannel(id);

                if (channel.IsValid())
                    channel.Get()->ProcessMessage(msg);
            }

            void DataRouter::OnMessageSent(uint64_t id)
            {
                IGNITE_UNUSED(id);
                // No-op.
            }

            void DataRouter::OnHandshakeSuccess(uint64_t id)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                connectedChannels.insert(id);
                channelsWaitPoint.NotifyAll();

                SP_DataChannel channel = FindChannelLocked(id);
                if (channel.IsValid())
                {
                    const IgniteNode& node = channel.Get()->GetNode();
                    if (!node.IsLegacy())
                        partChannels[node.GetGuid()] = channel;
                }
            }

            void DataRouter::OnHandshakeError(uint64_t id, const IgniteError& err)
            {
                IGNITE_UNUSED(id);

                common::concurrent::CsLockGuard lock(channelsMutex);

                lastHandshakeError.reset(new IgniteError(err));
                channelsWaitPoint.NotifyAll();
            }

            void DataRouter::OnNotificationHandlingError(uint64_t id, const IgniteError &err)
            {
                SP_DataChannel channel = FindChannel(id);

                if (channel.IsValid())
                    channel.Get()->Close(&err);
            }

            SP_DataChannel DataRouter::SyncMessage(Request &req, Response &rsp)
            {
                SP_DataChannel channel = GetRandomChannel();

                int32_t metaVer = typeMgr.GetVersion();

                channel = SyncMessagePreferredChannelNoMetaUpdate(req, rsp, channel);

                ProcessMeta(metaVer);

                return channel;
            }

            SP_DataChannel DataRouter::SyncMessage(Request &req, Response &rsp, const Guid &hint)
            {
                SP_DataChannel channel = GetBestChannel(hint);

                int32_t metaVer = typeMgr.GetVersion();

                channel = SyncMessagePreferredChannelNoMetaUpdate(req, rsp, channel);

                ProcessMeta(metaVer);

                return channel;
            }

            SP_DataChannel DataRouter::SyncMessageNoMetaUpdate(Request &req, Response &rsp)
            {
                SP_DataChannel channel = GetRandomChannel();

                channel = SyncMessagePreferredChannelNoMetaUpdate(req, rsp, channel);

                return channel;
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

            void DataRouter::CheckAffinity(Response &rsp)
            {
                const AffinityTopologyVersion* ver = rsp.GetAffinityTopologyVersion();

                if (ver != 0 && config.IsPartitionAwareness())
                    affinityManager.UpdateAffinity(*ver);
            }

            SP_DataChannel DataRouter::SyncMessagePreferredChannelNoMetaUpdate(Request &req, Response &rsp,
                const SP_DataChannel &preferred)
            {
                SP_DataChannel channel(preferred);

                if (!channel.IsValid())
                    channel = GetRandomChannel();

                if (!channel.IsValid())
                {
                    bool connected = EnsureConnected(config.GetConnectionTimeout());

                    if (!connected)
                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                            "Failed to establish connection with any host.");

                    channel = GetRandomChannel();
                    if (!channel.IsValid())
                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                            "Failed to establish connection with any host.");
                }

                try
                {
                    channel.Get()->SyncMessage(req, rsp, config.GetConnectionTimeout());
                }
                catch (IgniteError& err)
                {
                    InvalidateChannel(channel);

                    std::string msg("Connection failure during command processing. Please re-run command. Cause: ");
                    msg += err.GetText();

                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
                }

                CheckAffinity(rsp);

                return channel;
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

                InvalidateChannelLocked(channel);
            }

            void DataRouter::InvalidateChannelLocked(SP_DataChannel &channel)
            {
                if (!channel.IsValid())
                    return;

                DataChannel& channel0 = *channel.Get();
                channels.erase(channel0.GetId());
                partChannels.erase(channel0.GetNode().GetGuid());
            }

            SP_DataChannel DataRouter::GetRandomChannel()
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                return GetRandomChannelLocked();
            }

            SP_DataChannel DataRouter::GetRandomChannelLocked()
            {
                if (connectedChannels.empty())
                    return SP_DataChannel();

                int r = rand();

                size_t idx = r % connectedChannels.size();

                ChannelsIdSet::iterator it = connectedChannels.begin();

                std::advance(it, idx);

                return channels[*it];
            }

            SP_DataChannel DataRouter::GetBestChannel(const Guid& hint)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);

                ChannelsGuidMap::iterator itChannel = partChannels.find(hint);

                if (itChannel != partChannels.end())
                    return itChannel->second;

                return GetRandomChannelLocked();
            }

            void DataRouter::CollectAddresses(const std::string& str, std::vector<network::TcpRange>& ranges)
            {
                ranges.clear();

                utility::ParseAddress(str, ranges, DEFAULT_PORT);

                std::random_shuffle(ranges.begin(), ranges.end());
            }

            SP_DataChannel DataRouter::FindChannel(uint64_t id)
            {
                common::concurrent::CsLockGuard lock(channelsMutex);
                return FindChannelLocked(id);
            }

            SP_DataChannel DataRouter::FindChannelLocked(uint64_t id)
            {
                ChannelsIdMap::iterator it = channels.find(id);
                if (it != channels.end())
                    return it->second;

                return SP_DataChannel();
            }
        }
    }
}

