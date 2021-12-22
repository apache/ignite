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

#ifndef _IGNITE_NETWORK_WIN_ASYNC_CLIENT_POOL
#define _IGNITE_NETWORK_WIN_ASYNC_CLIENT_POOL

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
        /**
         * Windows-specific implementation of asynchronous client pool.
         */
        class WinAsyncClientPool : public AsyncClientPool
        {
        public:
            /**
             * Constructor
             */
            WinAsyncClientPool();

            /**
             * Destructor.
             */
            virtual ~WinAsyncClientPool();

            /**
             * Start internal thread that establishes connections to provided addresses and asynchronously sends and
             * receives messages from them. Function returns either when when thread is started and first connection is
             * established or failure happens.
             *
             * @param addrs Addresses to connect to.
             * @param handler Async event handler.
             * @param connLimit Connection upper limit. Zero means limit is disabled.
             * @throw IgniteError on error.
             */
            virtual void Start(const std::vector<TcpRange>& addrs, AsyncHandler& handler, uint32_t connLimit);

            /**
             * Close all established connections and stops handling thread.
             */
            virtual void Stop();

            /**
             * Send data to specific established connection.
             *
             * @param id Client ID.
             * @param mem Data to be sent.
             * @return @c true if connection is present and @c false otherwise.
             * @throw IgniteError on error.
             */
            virtual bool Send(uint64_t id, impl::interop::SP_InteropMemory mem);

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Event is issued to the handler with specified error.
             *
             * @param id Client ID.
             */
            virtual void Close(uint64_t id, const IgniteError* err);

        private:

            /**
             * Async pool connecting thread.
             */
            class ConnectingThread : public common::concurrent::Thread
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param clientPool Client pool.
                 */
                explicit ConnectingThread(WinAsyncClientPool& clientPool);

                /**
                 * Run thread.
                 */
                virtual void Run();

                /**
                 * Stop thread.
                 */
                void Stop();

                /**
                 * Wake up thread if it's sleeping.
                 */
                void WakeUp();

            private:
                /**
                 * Try establish connection to address in the range.
                 * @param range TCP range.
                 * @return New client.
                 */
                SP_WinAsyncClient TryConnect(const TcpRange& range);

                /**
                 * Try establish connection to address.
                 * @param addr Address.
                 * @return New client.
                 */
                static SP_WinAsyncClient TryConnect(const EndPoint& addr);

                /** Client pool. */
                WinAsyncClientPool& clientPool;

                /** Flag to signal that thread is stopping. */
                volatile bool stopping;

                /** Condition variable, which signalled when new connect is needed. */
                common::concurrent::ConditionVariable connectNeeded;
            };

            /**
             * Async pool worker thread.
             */
            class WorkerThread : public common::concurrent::Thread
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param clientPool Client pool.
                 */
                explicit WorkerThread(WinAsyncClientPool& clientPool);

                /**
                 * Run thread.
                 */
                virtual void Run();

                /**
                 * Stop thread.
                 */
                void Stop();

            private:
                /** Client pool. */
                WinAsyncClientPool& clientPool;

                /** Flag to signal that thread should stop. */
                volatile bool stopping;
            };

            /**
             * Close all established connections and stops handling threads.
             */
            void InternalStop();

            /**
             * Check whether a new connection should be established.
             *
             * @warning May only be called in clientsCs critical section.
             * @return @c true if a new connection should be established.
             */
            bool isConnectionNeededLocked() const;

            /**
             * Add client to connection map. Notify user.
             *
             * @param client Client.
             * @return Client ID.
             */
            uint64_t AddClient(SP_WinAsyncClient& client);

            /**
             * Closes and releases memory allocated for client with specified ID.
             *
             * @param id Client ID.
             * @return @c true if connection with specified ID was found.
             */
            bool CloseAndRelease(uint64_t id);

            /**
             * Closes and releases memory allocated for client with specified ID.
             * Error is reported to handler.
             *
             * @param id Client ID.
             * @param err Error to report. May be null.
             * @return @c true if connection with specified ID was found.
             */
            void CloseAndRelease(uint64_t id, const IgniteError* err);

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Error is not reported to handler.
             *
             * @param id Client ID.
             * @return @c true if connection with specified ID is found.
             */
            bool Close(uint64_t id);

            /**
             * Find client by ID.
             *
             * @warning Should only be called with clientsCs lock held.
             * @param id Client ID.
             * @return Client. Null pointer if is not found.
             */
            SP_WinAsyncClient FindClientLocked(uint64_t id) const;

            /**
             * Throw window specific error with error code.
             *
             * @param msg Error message.
             */
            static void ThrowSystemError(const std::string& msg);

            /** Flag indicating that pool is stopping. */
            volatile bool stopping;

            /** Event handler. */
            AsyncHandler* asyncHandler;

            /** Connecting thread. */
            ConnectingThread connectingThread;

            /** Internal thread. */
            WorkerThread workerThread;

            /** Upper limit of number of connections. */
            uint32_t connectionLimit;

            /** ID counter. */
            uint64_t idGen;

            /** IO Completion Port. Windows-specific primitive for asynchronous IO. */
            HANDLE iocp;

            /** Clients critical section. */
            common::concurrent::CriticalSection clientsCs;

            /** Clients condition variable. Notified when no clients are left. */
            common::concurrent::ConditionVariable clientsCv;

            /** Addresses to use for connection establishment. */
            std::vector<TcpRange> nonConnected;

            /** Client mapping ID -> client */
            std::map<uint64_t, SP_WinAsyncClient> clientIdMap;

            /** Client mapping EndPoint -> client */
            std::map<EndPoint, SP_WinAsyncClient> clientAddrMap;
        };

        // Type alias
        typedef common::concurrent::SharedPointer<network::AsyncClientPool> SP_AsyncClientPool;
    }
}

#endif //_IGNITE_NETWORK_WIN_ASYNC_CLIENT_POOL