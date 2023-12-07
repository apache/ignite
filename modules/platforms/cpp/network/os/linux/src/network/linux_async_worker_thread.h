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

#ifndef _IGNITE_NETWORK_LINUX_ASYNC_WORKER_THREAD
#define _IGNITE_NETWORK_LINUX_ASYNC_WORKER_THREAD

#include <time.h>

#include <stdint.h>
#include <memory>

#include <ignite/common/concurrent.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/async_handler.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

#include "network/linux_async_client.h"
#include "network/connecting_context.h"

namespace ignite
{
    namespace network
    {
        class LinuxAsyncClientPool;

        /**
         * Async pool working thread.
         */
        class LinuxAsyncWorkerThread : protected common::concurrent::Thread
        {
        public:
            /**
             * Default constructor.
             */
            LinuxAsyncWorkerThread(LinuxAsyncClientPool& clientPool);

            /**
             * Destructor.
             */
            virtual ~LinuxAsyncWorkerThread();

            /**
             * Start worker thread.
             *
             * @param limit Connection limit.
             * @param addrs Addresses to connect to.
             */
            void Start0(size_t limit, const std::vector<TcpRange>& addrs);

            /**
             * Stop thread.
             */
            void Stop();

        private:
            /**
             * Run thread.
             */
            virtual void Run();

            /**
             * Initiate new connection process if needed.
             */
            void HandleNewConnections();

            /**
             * Handle epoll events.
             */
            void HandleConnectionEvents();

            /**
             * Handle network error during connection establishment.
             *
             * @param addr End point.
             * @param msg Error message.
             */
            void ReportConnectionError(const EndPoint& addr, const std::string& msg);

            /**
             * Handle failed connection.
             *
             * @param msg Error message.
             */
            void HandleConnectionFailed(const std::string& msg);

            /**
             * Handle network error on established connection.
             *
             * @param client Client instance.
             */
            void HandleConnectionClosed(LinuxAsyncClient* client);

            /**
             * Handle successfully established connection.
             *
             * @param client Client instance.
             */
            void HandleConnectionSuccess(LinuxAsyncClient* client);

            /**
             * Calculate connection timeout.
             *
             * @return Connection timeout.
             */
            int CalculateConnectionTimeout() const;

            /**
             * Check whether new connection should be initiated.
             *
             * @return @c true if new connection should be initiated.
             */
            bool ShouldInitiateNewConnection() const;

            /** Client pool. */
            LinuxAsyncClientPool& clientPool;

            /** Flag indicating that thread is stopping. */
            bool stopping;

            /** Client epoll file descriptor. */
            int epoll;

            /** Stop event file descriptor. */
            int stopEvent;

            /** Addresses to use for connection establishment. */
            std::vector<TcpRange> nonConnected;

            /** Connection which is currently in connecting process. */
            std::auto_ptr<ConnectingContext> currentConnection;

            /** Currently connected client. */
            SP_LinuxAsyncClient currentClient;

            /** Failed connection attempts. */
            size_t failedAttempts;

            /** Last connection time. */
            timespec lastConnectionTime;

            /** Minimal number of addresses. */
            size_t minAddrs;
        };
    }
}

#endif //_IGNITE_NETWORK_LINUX_ASYNC_WORKER_THREAD
