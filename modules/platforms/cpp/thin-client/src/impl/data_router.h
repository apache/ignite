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
#include <ignite/impl/binary/binary_writer_impl.h>

#include "impl/data_channel.h"
#include "impl/net/end_point.h"
#include "impl/net/tcp_range.h"
#include "impl/cache/cache_affinity_info.h"

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
            public:
                /** Connection establishment timeout in seconds. */
                enum { DEFALT_CONNECT_TIMEOUT = 5 };

                /** Network IO operation timeout in seconds. */
                enum { DEFALT_IO_TIMEOUT = 5 };

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
                 * Synchronously send request message and receive response.
                 * Uses provided timeout.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                void SyncMessage(const ReqT& req, RspT& rsp)
                {
                    SP_DataChannel channel = GetRandomChannel();

                    int32_t metaVer = typeMgr.GetVersion();

                    channel.Get()->SyncMessage(req, rsp, ioTimeout);

                    if (typeMgr.IsUpdatedSince(metaVer))
                    {
                        IgniteError err;

                        if (!typeMgr.ProcessPendingUpdates(err))
                            throw err;
                    }
                }

                /**
                 * Synchronously send request message and receive response.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @param hint End points of the preferred server node to use.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                void SyncMessage(const ReqT& req, RspT& rsp, const std::vector<net::EndPoint>& hint)
                {
                    SP_DataChannel channel = GetBestChannel(hint);

                    int32_t metaVer = typeMgr.GetVersion();

                    channel.Get()->SyncMessage(req, rsp, ioTimeout);

                    if (typeMgr.IsUpdatedSince(metaVer))
                    {
                        IgniteError err;
                        
                        if (!typeMgr.ProcessPendingUpdates(err))
                            throw err;
                    }
                }

                /**
                 * Synchronously send request message and receive response.
                 * Does not update metadata.
                 * Uses provided timeout.
                 *
                 * @param req Request message.
                 * @param rsp Response message.
                 * @throw IgniteError on error.
                 */
                template<typename ReqT, typename RspT>
                void SyncMessageNoMetaUpdate(const ReqT& req, RspT& rsp)
                {
                    SP_DataChannel channel = GetRandomChannel();

                    channel.Get()->SyncMessage(req, rsp, ioTimeout);
                }

                /**
                 * Update affinity mapping for the cache.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Cache binary flag.
                 */
                void RefreshAffinityMapping(int32_t cacheId, bool binary);

                /**
                 * Get affinity mapping for the cache.
                 *
                 * @param cacheId Cache ID.
                 * @return Mapping.
                 */
                cache::SP_CacheAffinityInfo GetAffinityMapping(int32_t cacheId);

                /**
                 * Clear affinity mapping for the cache.
                 *
                 * @param cacheId Cache ID.
                 */
                void ReleaseAffinityMapping(int32_t cacheId);

            private:
                IGNITE_NO_COPY_ASSIGNMENT(DataRouter);

                /** End point collection. */
                typedef std::vector<net::EndPoint> EndPoints;

                /** Shared pointer to end points. */
                typedef common::concurrent::SharedPointer<EndPoints> SP_EndPoints;

                /**
                 * Get endpoints for the key.
                 * Always using Rendezvous Affinity Function algorithm for now.
                 *
                 * @param cacheId Cache ID.
                 * @param key Key.
                 * @return Endpoints for the key.
                 */
                int32_t GetPartitionForKey(int32_t cacheId, const WritableKey& key);

                /**
                 * Get random data channel.
                 *
                 * @return Random data channel.
                 */
                SP_DataChannel GetRandomChannel();

                /**
                 * Check whether the provided address hint is the local host.
                 *
                 * @param hint Hint.
                 * @return @c true if the local host.
                 */
                bool IsLocalHost(const std::vector<net::EndPoint>& hint);

                /**
                 * Check whether the provided address is the local host.
                 *
                 * @param host Host.
                 * @return @c true if the local host.
                 */
                static bool IsLocalAddress(const std::string& host);

                /**
                 * Check whether the provided end point is provided by user using configuration.
                 *
                 * @param endPoint End point to check.
                 * @return @c true if provided by user using configuration. 
                 */
                bool IsProvidedByUser(const net::EndPoint& endPoint);

                /**
                 * Get the best data channel.
                 *
                 * @param hint End points of the preferred server node to use.
                 * @return The best available data channel.
                 */
                SP_DataChannel GetBestChannel(const std::vector<net::EndPoint>& hint);

                /**
                 * Update local addresses.
                 */
                void UpdateLocalAddresses();

                /**
                 * Collect all addresses from string.
                 *
                 * @param str String with connection strings to parse.
                 * @param ranges Address ranges.
                 */
                static void CollectAddresses(const std::string& str, std::vector<net::TcpRange>& ranges);

                /** IO timeout in seconds. */
                int32_t ioTimeout;

                /** Connection timeout in seconds. */
                int32_t connectionTimeout;

                /** Configuration. */
                ignite::thin::IgniteClientConfiguration config;

                /** Address ranges. */
                std::vector<net::TcpRange> ranges;

                /** Local addresses. */
                std::set<std::string> localAddresses;

                /** Type updater. */
                std::auto_ptr<binary::BinaryTypeUpdater> typeUpdater;

                /** Metadata manager. */
                binary::BinaryTypeManager typeMgr;

                /** Data channels. */
                std::map<net::EndPoint, SP_DataChannel> channels;

                /** Channels mutex. */
                common::concurrent::CriticalSection channelsMutex;

                /** Cache affinity mapping. */
                std::map<int32_t, cache::SP_CacheAffinityInfo> cacheAffinityMapping;

                /** Cache affinity mapping mutex. */
                common::concurrent::CriticalSection cacheAffinityMappingMutex;
            };

            /** Shared pointer type. */
            typedef common::concurrent::SharedPointer<DataRouter> SP_DataRouter;
        }
    }
}

#endif //_IGNITE_IMPL_THIN_DATA_ROUTER
