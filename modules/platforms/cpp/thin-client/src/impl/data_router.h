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

#ifndef _IGNITE_IMPL_THIN_DATA_ROUTER
#define _IGNITE_IMPL_THIN_DATA_ROUTER

#include <stdint.h>

#include <map>
#include <set>
#include <vector>
#include <memory>
#include <string>

#include <ignite/thin/ignite_client_configuration.h>

#include <ignite/common/concurrent.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include "impl/affinity/affinity_assignment.h"
#include "impl/affinity/affinity_manager.h"
#include "impl/data_channel.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            // Forward declaration.
            class WritableKey;

            /**
             * Data router.
             *
             * Ensures there is a connection between client and one of the servers
             * and routes data between them.
             */
            class DataRouter
            {
                typedef std::map<Guid, SP_DataChannel> ChannelsGuidMap;
                typedef std::vector<SP_DataChannel> ChannelsVector;

            public:
                /** Connection establishment timeout in seconds. */
                enum { DEFAULT_CONNECT_TIMEOUT = 5 };

                /** Network IO operation timeout in seconds. */
                enum { DEFAULT_IO_TIMEOUT = 5 };

                /** Default port. */
                enum { DEFAULT_PORT = 10800 };

                /**
                 * Constructor.
                 *
                 * @param cfg Configuration.
                 */
                DataRouter(const ignite::thin::IgniteClientConfiguration& cfg);

                /**
                 * Destructor.
                 */
                ~DataRouter();

                /**
                 * Establish connection to cluster.
                 */
                void Connect();

                /**
                 * Close connection.
                 */
                void Close();

                /**
                 * Update affinity if needed.
                 *
                 * @param rsp Response.
                 */
                template <typename RspT>
                void CheckAffinity(RspT& rsp)
                {
                    const AffinityTopologyVersion* ver = rsp.GetAffinityTopologyVersion();

                    if (ver != 0 && config.IsPartitionAwareness())
                        affinityManager.UpdateAffinity(*ver);
                }

                /**
                 * Process meta if needed.
                 *
                 * @param metaVer Version of meta.
                 */
                void ProcessMeta(int32_t metaVer);

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @return Channel that was used for request.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                SP_DataChannel SyncMessage(const ReqT& req, RspT& rsp)
                {
                    SP_DataChannel channel = GetRandomChannel();

                    int32_t metaVer = typeMgr.GetVersion();

                    SyncMessagePreferredChannelNoMetaUpdate(req, rsp, channel);

                    ProcessMeta(metaVer);

                    return channel;
                }

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param hint Preferred server node to use.
                 * @return Channel that was used for request.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                SP_DataChannel SyncMessage(const ReqT& req, RspT& rsp, const Guid& hint)
                {
                    SP_DataChannel channel = GetBestChannel(hint);

                    int32_t metaVer = typeMgr.GetVersion();

                    SyncMessagePreferredChannelNoMetaUpdate(req, rsp, channel);

                    ProcessMeta(metaVer);

                    return channel;
                }

                /**
                 * Synchronously send request message and receive response.
                 * Does not update metadata.
                 * Uses provided timeout.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @return Channel that was used for request.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                SP_DataChannel SyncMessageNoMetaUpdate(const ReqT& req, RspT& rsp)
                {
                    SP_DataChannel channel = GetRandomChannel();

                    SyncMessagePreferredChannelNoMetaUpdate(req, rsp, channel);

                    return channel;
                }

                /**
                 * Synchronously send request message, receive response and get a notification.
                 *
                 * @param req Request message.
                 * @param notification Notification message.
                 * @return Channel that was used for request.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename NotT>
                SP_DataChannel SyncMessageWithNotification(const ReqT& req, NotT& notification)
                {
                    SP_DataChannel channel = GetRandomChannel();

                    if (!channel.IsValid())
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                                          "Can not connect to any available cluster node. Please restart client");
                    }

                    int32_t metaVer = typeMgr.GetVersion();

                    try
                    {
                        channel.Get()->SyncMessageWithNotification(req, notification, ioTimeout);
                    }
                    catch (IgniteError& err)
                    {
                        InvalidateChannel(channel);

                        std::string msg("Connection failure during command processing. Please re-run command. Cause: ");
                        msg += err.GetText();

                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
                    }

                    ProcessMeta(metaVer);

                    return channel;
                }

                /**
                 * Update affinity mapping for the cache.
                 *
                 * @param cacheId Cache ID.
                 */
                void RefreshAffinityMapping(int32_t cacheId);

                /**
                 * Update affinity mapping for caches.
                 *
                 * @param cacheIds Cache IDs.
                 */
                void RefreshAffinityMapping(const std::vector<int32_t>& cacheIds);

                /**
                 * Checked whether partition awareness enabled.
                 *
                 * @return @c true if partition awareness enabled.
                 */
                bool IsPartitionAwarenessEnabled() const
                {
                    return config.IsPartitionAwareness();
                }

                /**
                 * Get affinity mapping for the cache.
                 *
                 * @param cacheId Cache ID.
                 * @return Mapping.
                 */
                affinity::SP_AffinityAssignment GetAffinityAssignment(int32_t cacheId) const;

                /**
                 * Get IO timeout.
                 *
                 * @return IO timeout.
                 */
                int32_t GetIoTimeout()
                {
                    return ioTimeout;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataRouter);

                /**
                 * Invalidate provided data channel.
                 *
                 * @param channel Data channel.
                 */
                void InvalidateChannel(SP_DataChannel& channel);

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param preferred Preferred channel to use.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                void SyncMessagePreferredChannelNoMetaUpdate(const ReqT& req, RspT& rsp, const SP_DataChannel& preferred)
                {
                    SP_DataChannel channel = preferred;

                    if (!channel.IsValid())
                        channel = GetRandomChannel();

                    if (!channel.IsValid())
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                            "Can not connect to any available cluster node. Please restart client");
                    }

                    try
                    {
                        channel.Get()->SyncMessage(req, rsp, ioTimeout);
                    }
                    catch (IgniteError& err)
                    {
                        InvalidateChannel(channel);

                        std::string msg("Connection failure during command processing. Please re-run command. Cause: ");
                        msg += err.GetText();

                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
                    }

                    CheckAffinity(rsp);
                }

                /** Shared pointer to end points. */
                typedef common::concurrent::SharedPointer<network::EndPoints> SP_EndPoints;

                /**
                 * Get random data channel.
                 *
                 * @return Random data channel or null, if not connected.
                 */
                SP_DataChannel GetRandomChannel();

                /**
                 * Get random data channel.
                 * @warning May only be called when lock is held!
                 *
                 * @return Random data channel or null, if not connected.
                 */
                SP_DataChannel GetRandomChannelUnsafe();

                /**
                 * Check whether the provided end point is provided by user using configuration.
                 *
                 * @param endPoint End point to check.
                 * @return @c true if provided by user using configuration.
                 */
                bool IsProvidedByUser(const network::EndPoint& endPoint);

                /**
                 * Get the best data channel.
                 *
                 * @param hint GUID of preferred server node to use.
                 * @return The best available data channel or null if not connected.
                 */
                SP_DataChannel GetBestChannel(const Guid& hint);

                /**
                 * Collect all addresses from string.
                 *
                 * @param str String with connection strings to parse.
                 * @param ranges Address ranges.
                 */
                static void CollectAddresses(const std::string& str, std::vector<network::TcpRange>& ranges);

                /** IO timeout in seconds. */
                int32_t ioTimeout;

                /** Connection timeout in seconds. */
                int32_t connectionTimeout;

                /** Configuration. */
                ignite::thin::IgniteClientConfiguration config;

                /** Address ranges. */
                std::vector<network::TcpRange> ranges;

                /** Type updater. */
                std::auto_ptr<binary::BinaryTypeUpdater> typeUpdater;

                /** Metadata manager. */
                binary::BinaryTypeManager typeMgr;

                /** Data channels. */
                ChannelsGuidMap channels;

                /** Data channels. */
                ChannelsVector legacyChannels;

                /** Channels mutex. */
                common::concurrent::CriticalSection channelsMutex;

                /** Cache affinity manager. */
                affinity::AffinityManager affinityManager;
            };

            /** Shared pointer type. */
            typedef common::concurrent::SharedPointer<DataRouter> SP_DataRouter;
        }
    }
}

#endif //_IGNITE_IMPL_THIN_DATA_ROUTER
