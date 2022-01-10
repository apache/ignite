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

#ifndef _IGNITE_NETWORK_WIN_ASYNC_CLIENT
#define _IGNITE_NETWORK_WIN_ASYNC_CLIENT

#include "network/sockets.h"

#include <stdint.h>
#include <deque>

#include <ignite/common/concurrent.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/async_handler.h>
#include <ignite/network/codec.h>
#include <ignite/network/end_point.h>
#include <ignite/network/tcp_range.h>

namespace ignite
{
    namespace network
    {
        /**
         * Operation kind.
         */
        struct IoOperationKind
        {
            enum Type
            {
                SEND,

                RECEIVE,
            };
        };

        /**
         * Represents single IO operation.
         * Needed to be able to distinguish one operation from another.
         */
        struct IoOperation
        {
            /** Overlapped structure that should be passed to every IO operation. */
            WSAOVERLAPPED overlapped;

            /** Operation type. */
            IoOperationKind::Type kind;
        };

        /**
         * Windows-specific implementation of async network client.
         */
        class WinAsyncClient
        {
        public:
            /**
             * State.
             */
            struct State
            {
                enum Type
                {
                    CONNECTED,

                    IN_POOL,

                    SHUTDOWN,

                    CLOSED,
                };
            };

            /**
             * Constructor.
             *
             * @param socket Socket.
             * @param addr Address.
             * @param range Range.
             * @param bufLen Buffer length.
             */
            WinAsyncClient(SOCKET socket, const EndPoint& addr, const TcpRange& range, int32_t bufLen);

            /**
             * Destructor.
             */
            ~WinAsyncClient();

            /**
             * Shutdown client.
             *
             * Can be called from external threads.
             * Can be called from WorkerThread.
             *
             * @param err Error message. Can be null.
             * @return @c true if shutdown performed successfully.
             */
            bool Shutdown(const IgniteError* err);

            /**
             * Close client.
             *
             * Should not be called from external threads.
             * Can be called from WorkerThread.
             *
             * @return @c true if shutdown performed successfully.
             */
            bool Close();

            /**
             * Add client to IOCP.
             *
             * @return IOCP handle on success and NULL otherwise.
             */
            HANDLE AddToIocp(HANDLE iocp);

            /**
             * Send packet using client.
             *
             * @param data Data to send.
             * @return @c true on success.
             */
            bool Send(const DataBuffer& data);

            /**
             * Initiate next receive of data.
             *
             * @return @c true on success.
             */
            bool Receive();

            /**
             * Get client ID.
             *
             * @return Client ID.
             */
            uint64_t GetId() const
            {
                return id;
            }

            /**
             * Set ID.
             *
             * @param id ID to set.
             */
            void SetId(uint64_t id)
            {
                this->id = id;
            }

            /**
             * Get address.
             *
             * @return Address.
             */
            const EndPoint& GetAddress() const
            {
                return addr;
            }

            /**
             * Get range.
             *
             * @return Range.
             */
            const TcpRange& GetRange() const
            {
                return range;
            }

            /**
             * Check whether client is closed.
             *
             * @return @c true if closed.
             */
            bool IsClosed() const
            {
                return socket == NULL;
            }

            /**
             * Process sent data.
             *
             * @param bytes Bytes.
             * @return @c true on success.
             */
            bool ProcessSent(size_t bytes);

            /**
             * Process received bytes.
             *
             * @param bytes Number of received bytes.
             */
            DataBuffer ProcessReceived(size_t bytes);

            /**
             * Get closing error for the connection. Can be IGNITE_SUCCESS.
             *
             * @return Connection error.
             */
            const IgniteError& GetCloseError() const
            {
                return closeErr;
            }

        private:

            /**
             * Clears client's receive buffer.
             *
             * @return Data received so far.
             */
            void ClearReceiveBuffer();

            /**
             * Send next packet in queue.
             *
             * @warning Can only be called when holding sendCs lock.
             * @return @c true on success.
             */
            bool SendNextPacketLocked();

            /** Buffer length. */
            int32_t bufLen;

            /** Client state. */
            State::Type state;

            /** Socket. */
            SOCKET socket;

            /** Connection ID. */
            uint64_t id;

            /** Server end point. */
            EndPoint addr;

            /** Address range associated with current connection. */
            TcpRange range;

            /** Current send operation. */
            IoOperation currentSend;

            /** Packets that should be sent. */
            std::deque<DataBuffer> sendPackets;

            /** Send critical section. */
            common::concurrent::CriticalSection sendCs;

            /** Current receive operation. */
            IoOperation currentRecv;

            /** Packet that is currently received. */
            impl::interop::SP_InteropMemory recvPacket;

            /** Closing error. */
            IgniteError closeErr;
        };

        /** Shared pointer to async client. */
        typedef common::concurrent::SharedPointer<WinAsyncClient> SP_WinAsyncClient;
    }
}

#endif //_IGNITE_NETWORK_WIN_ASYNC_CLIENT