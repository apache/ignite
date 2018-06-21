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

#include <sstream>
#include <iterator>
#include <algorithm>

#include <ignite/impl/thin/utility.h>
#include <ignite/impl/thin/data_router.h>
#include <ignite/impl/thin/message.h>
#include <ignite/impl/thin/ssl/ssl_gateway.h>
#include <ignite/impl/thin/net/remote_type_updater.h>


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
                typeUpdater(),
                typeMgr()
            {
                typeUpdater.reset(new net::RemoteTypeUpdater(*this));
            }

            DataRouter::~DataRouter()
            {
                // No-op.
            }

            void DataRouter::Connect()
            {
                using ignite::thin::SslMode;

                if (config.GetEndPoints().empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "No valid address to connect.");

                std::vector<net::TcpRange> ranges;

                CollectAddresses(config.GetEndPoints(), ranges);

                SP_DataChannel channel(new DataChannel(config, typeMgr));

                bool connected = false;

                for (std::vector<net::TcpRange>::iterator it = ranges.begin(); it != ranges.end(); ++it)
                {
                    net::TcpRange& range = *it;

                    for (uint16_t port = range.port; port <= range.port + range.range; ++port)
                    {
                        connected = channel.Get()->Connect(range.host, port, connectionTimeout);

                        if (connected)
                            break;
                    }
                }

                if (!connected)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to establish connection with any host.");

                common::concurrent::CsLockGuard lock(channelsMutex);

                channels[channel.Get()->GetAddress()].Swap(channel);
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
                // TODO

                return false;
            }

            bool DataRouter::IsLocalAddress(const std::string& host)
            {
                // TODO

                return false;
            }

            bool DataRouter::IsProvidedByUser(const net::EndPoint& endPoint)
            {
                // TODO

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

            void DataRouter::CollectAddresses(const std::string& str, std::vector<net::TcpRange>& ranges)
            {
                ranges.clear();

                utility::ParseAddress(str, ranges, DEFAULT_PORT);

                std::random_shuffle(ranges.begin(), ranges.end());
            }
        }
    }
}

