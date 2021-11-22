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

#ifndef _IGNITE_NETWORK_ASYNC_CLIENT_POOL
#define _IGNITE_NETWORK_ASYNC_CLIENT_POOL

#include <stdint.h>

#include <vector>

#include <ignite/future.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

namespace ignite
{
    namespace network
    {
        class AsyncClientPool
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AsyncClientPool()
            {
                // No-op.
            }

            /**
             * Start internal thread that establishes connections to provided addresses and asynchronously sends and
             * receives messages from them. Function returns either when when thread is started and first connection is
             * established or failure happens.
             *
             * @param addrs Addresses to connect to.
             * @param handler Async event handler.
             * @param timeout Connection establishment timeout.
             * @throw IgniteError on error.
             */
            virtual void Start(const std::vector<TcpRange>& addrs, AsyncHandler& handler, int32_t timeout) = 0;

            /**
             * Close all established connections and stops handling thread.
             */
            virtual void Close() = 0;

            /**
             * Send data to specific established connection.
             *
             * @param id Client ID.
             * @param data Pointer to data to be sent.
             * @param size Size of the data in bytes.
             * @param timeout Timeout.
             * @return @c true if connection is present and @c false otherwise.
             * @throw IgniteError on error.
             */
            virtual bool Send(uint64_t id, const int8_t* data, size_t size, int32_t timeout) = 0;

            /**
             * Send data using randomly chosen established connection.
             *
             * @param data Pointer to data to be sent.
             * @param size Size of the data in bytes.
             * @param timeout Timeout.
             * @throw IgniteError on error.
             */
            virtual void Send(const int8_t* data, size_t size, int32_t timeout) = 0;
        };
    }
}

#endif //_IGNITE_NETWORK_ASYNC_CLIENT_POOL