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
#include <memory>

#include <ignite/ignite_error.h>

#include <ignite/common/concurrent.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/async_client_pool.h>
#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

namespace ignite
{
    namespace network
    {
        struct IoOperationKind
        {
            enum Type
            {
                SEND,

                RECEIVE,
            };
        };

        struct IoOperation
        {
            /** Overlapped structure that should be passed to every IO operation. */
            WSAOVERLAPPED overlapped;

            /** Operation type. */
            IoOperationKind::Type kind;

            /** Bytes to be transferred. */
            size_t toTransfer;

            /** Already transferred data in bytes. */
            size_t transferredSoFar;

            /** Packet to be transferred. */
            impl::interop::SP_InteropMemory packet;
        };

        /**
         * Client state.
         */
        struct WinAsyncClientState
        {
            enum Type
            {
                DISCONNECTED,

                CONNECTING,

                CONNECTED,

                SENDING,

                RECEIVING_HEADER,

                RECEIVING_BODY,
            };
        };

        /**
         * Windows-specific implementation of async network client.
         */
        struct WinAsyncClient
        {
            WinAsyncClient(SOCKET socket, const EndPoint& addr);

            WinAsyncClient();

            ~WinAsyncClient();

            void Close();

            SOCKET socket;
            uint64_t id;
            EndPoint addr;
            TcpRange range;
            WinAsyncClientState::Type state;
        };

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
             * @param timeout Timeout.
             * @return @c true if connection is present and @c false otherwise.
             * @throw IgniteError on error.
             */
            virtual bool Send(uint64_t id, impl::interop::SP_InteropMemory mem, int32_t timeout);

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Event is issued to the handler with specified error.
             *
             * @param id Client ID.
             */
            virtual void Close(uint64_t id, const IgniteError* err);

        private:

            /** Shared pointer to async client. */
            typedef common::concurrent::SharedPointer<WinAsyncClient> SP_WinAsyncClient;

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

                /** Flag to signal that thread is stopping. */
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
            bool isConnectionNeeded() const;

            /**
             * Add client to connection map. Notify user.
             * @param client Client.
             * @return Client ID.
             */
            uint64_t AddClient(SP_WinAsyncClient& client);

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Error is not reported to handler.
             *
             * @param id Client ID.
             * @return @c true if connection with specified ID is found.
             */
            bool Reset(uint64_t id);

            /**
             * Throw window specific error with error code.
             */
            static void ThrowWindowsError(const std::string& msg);

            /**
             * Throw WSA specific error with error code.
             */
            static void ThrowWsaError(const std::string& msg);

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