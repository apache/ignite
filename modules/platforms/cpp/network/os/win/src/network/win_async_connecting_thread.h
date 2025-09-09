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

#ifndef _IGNITE_NETWORK_WIN_ASYNC_CONNECTING_THREAD
#define _IGNITE_NETWORK_WIN_ASYNC_CONNECTING_THREAD

#include <stdint.h>

#include <ignite/ignite_error.h>

#include <ignite/common/concurrent.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

#include "network/win_async_client.h"

namespace ignite
{
    namespace network
    {
        class WinAsyncClientPool;

        /**
         * Async pool connecting thread.
         */
        class WinAsyncConnectingThread : protected common::concurrent::Thread
        {
            /** Send and receive buffers size. */
            enum { BUFFER_SIZE = 0x10000 };

        public:
            /**
             * Constructor.
             */
            explicit WinAsyncConnectingThread();

            /**
             * Start thread.
             *
             * @param clientPool Client pool.
             * @param limit Connection limit.
             * @param addrs Addresses.
             */
            void Start(WinAsyncClientPool& clientPool, size_t limit, const std::vector<TcpRange>& addrs);

            /**
             * Stop thread.
             */
            void Stop();

            /**
             * Notify about new address available for connection.
             *
             * @param range Address range.
             */
            void NotifyFreeAddress(const TcpRange &range);

        private:
            /**
             * Run thread.
             */
            virtual void Run();

            /**
             * Try establish connection to address in the range.
             * @param range TCP range.
             * @return New client.
             */
            static SP_WinAsyncClient TryConnect(const TcpRange& range);

            /**
             * Try establish connection to address.
             * @param addr Address.
             * @return Socket.
             */
            static SOCKET TryConnect(const EndPoint& addr);

            /**
             * Get random address.
             *
             * @warning Will block if no addresses are available for connect.
             * @return @c true if a new connection should be established.
             */
            TcpRange GetRandomAddress() const;

            /** Client pool. */
            WinAsyncClientPool* clientPool;

            /** Flag to signal that thread is stopping. */
            volatile bool stopping;

            /** Failed connection attempts. */
            size_t failedAttempts;

            /** Minimal number of addresses. */
            size_t minAddrs;

            /** Addresses critical section. */
            mutable common::concurrent::CriticalSection addrsCs;

            /** Condition variable, which signalled when new connect is needed. */
            mutable common::concurrent::ConditionVariable connectNeeded;

            /** Addresses to use for connection establishment. */
            std::vector<TcpRange> nonConnected;
        };
    }
}

#endif //_IGNITE_NETWORK_WIN_ASYNC_CONNECTING_THREAD