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
#include <ignite/common/thread_pool.h>
#include <ignite/common/promise.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>
#include <ignite/network/async_client_pool.h>
#include <ignite/impl/binary/binary_writer_impl.h>

#include "impl/affinity/affinity_assignment.h"
#include "impl/affinity/affinity_manager.h"
#include "impl/channel_state_handler.h"
#include "impl/data_channel.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            // Forward declaration.
            class WritableKey;

            // Forward declaration.
            class Request;

            // Forward declaration.
            class Response;

            /**
             * Data router.
             *
             * Ensures there is a connection between client and one of the servers
             * and routes data between them.
             */
            class DataRouter : public network::AsyncHandler, public ChannelStateHandler
            {
                typedef std::map<Guid, SP_DataChannel> ChannelsGuidMap;
                typedef std::map<uint64_t, SP_DataChannel> ChannelsIdMap;
                typedef std::set<uint64_t> ChannelsIdSet;

            public:
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
                 * Callback that called on successful connection establishment.
                 *
                 * @param addr Address of the new connection.
                 * @param id Connection ID.
                 */
                virtual void OnConnectionSuccess(const network::EndPoint& addr, uint64_t id);

                /**
                 * Callback that called on error during connection establishment.
                 *
                 * @param addr Connection address.
                 * @param err Error.
                 */
                virtual void OnConnectionError(const network::EndPoint& addr, const IgniteError& err);

                /**
                 * Callback that called on error during connection establishment.
                 *
                 * @param id Async client ID.
                 * @param err Error.
                 */
                virtual void OnConnectionClosed(uint64_t id, const IgniteError* err);

                /**
                 * Callback that called when new message is received.
                 *
                 * @param id Async client ID.
                 * @param msg Received message.
                 */
                virtual void OnMessageReceived(uint64_t id, const network::DataBuffer& msg);

                /**
                 * Callback that called when message is sent.
                 *
                 * @param id Async client ID.
                 */
                virtual void OnMessageSent(uint64_t id);

                /**
                 * Channel handshake completion callback.
                 *
                 * @param id Channel ID.
                 */
                virtual void OnHandshakeSuccess(uint64_t id);

                /**
                 * Channel handshake error callback.
                 *
                 * @param id Channel ID.
                 * @param err Error.
                 */
                virtual void OnHandshakeError(uint64_t id, const IgniteError& err);

                /**
                 * Called if notification handling failed.
                 *
                 * @param id Channel ID.
                 * @param err Error.
                 */
                virtual void OnNotificationHandlingError(uint64_t id, const IgniteError& err);

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @return Channel that was used for request.
                 * @throw IgniteError on error.
                 */
                SP_DataChannel SyncMessage(Request& req, Response& rsp);

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param hint Preferred server node to use.
                 * @return Channel that was used for request.
                 * @throw IgniteError on error.
                 */
                SP_DataChannel SyncMessage(Request& req, Response& rsp, const Guid& hint);

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
                SP_DataChannel SyncMessageNoMetaUpdate(Request& req, Response& rsp);

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
                int32_t GetIoTimeout() const
                {
                    return config.GetConnectionTimeout();
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataRouter);

                /**
                 * Make sure that there is at least one connection to a cluster. Wait for specified timeout.
                 * @param timeout Timeout.
                 * @return @c true if connected, @c false otherwise.
                 */
                bool EnsureConnected(int32_t timeout);

                /**
                 * Invalidate provided data channel.
                 *
                 * @param channel Data channel.
                 */
                void InvalidateChannel(SP_DataChannel& channel);

                /**
                 * Invalidate provided data channel.
                 *
                 * @warning Should be only called with locked channelsMutex.
                 * @param channel Data channel.
                 */
                void InvalidateChannelLocked(SP_DataChannel& channel);

                /**
                 * Process meta if needed.
                 *
                 * @param metaVer Version of meta.
                 */
                void ProcessMeta(int32_t metaVer);

                /**
                 * Update affinity if needed.
                 *
                 * @param rsp Response.
                 */
                void CheckAffinity(Response& rsp);

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param preferred Preferred channel to use.
                 * @throw IgniteError on error.
                 *
                 * @return Data channel that was used.
                 */
                SP_DataChannel SyncMessagePreferredChannelNoMetaUpdate(Request& req, Response& rsp,
                    const SP_DataChannel& preferred);

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
                SP_DataChannel GetRandomChannelLocked();

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

                /**
                 * Check whether there were any critical errors during handshake.
                 * @warning May only be called when lock is held!
                 *
                 * @throw IgniteError if there is error.
                 */
                void CheckHandshakeErrorLocked();

                /**
                 * Find channel by ID.
                 *
                 * @param id Channel ID
                 * @return Channel or null if is not present.
                 */
                SP_DataChannel FindChannel(uint64_t id);

                /**
                 * Find channel by ID.
                 * @warning May only be called when lock is held!
                 *
                 * @param id Channel ID
                 * @return Channel or null if is not present.
                 */
                SP_DataChannel FindChannelLocked(uint64_t id);

                /** Configuration. */
                ignite::thin::IgniteClientConfiguration config;

                /** Address ranges. */
                std::vector<network::TcpRange> ranges;

                /** Async client pool */
                network::SP_AsyncClientPool asyncPool;

                /** Type updater. */
                std::auto_ptr<binary::BinaryTypeUpdater> typeUpdater;

                /** Metadata manager. */
                binary::BinaryTypeManager typeMgr;

                /** All data channels. */
                ChannelsIdMap channels;

                /** Partition awareness data channels. */
                ChannelsGuidMap partChannels;

                /** Channel that complete handshake successfully. */
                ChannelsIdSet connectedChannels;

                /** Channels mutex. */
                common::concurrent::CriticalSection channelsMutex;

                /** Channels connection wait point. */
                common::concurrent::ConditionVariable channelsWaitPoint;

                /** Last handshake error. */
                std::auto_ptr<IgniteError> lastHandshakeError;

                /** Cache affinity manager. */
                affinity::AffinityManager affinityManager;

                /** Thread pool to dispatch user code execution. */
                common::ThreadPool userThreadPool;
            };

            /** Shared pointer type. */
            typedef common::concurrent::SharedPointer<DataRouter> SP_DataRouter;
        }
    }
}

#endif //_IGNITE_IMPL_THIN_DATA_ROUTER
