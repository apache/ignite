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
            enum
            {
                /** Packet length header size. */
                PACKET_HEADER_SIZE = 4
            };

            /** Overlapped structure that should be passed to every IO operation. */
            WSAOVERLAPPED overlapped;

            /** Operation type. */
            IoOperationKind::Type kind;

            /** Bytes to be transferred. */
            size_t toTransfer;

            /** Already transferred data in bytes. */
            size_t transferredSoFar;
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
             */
            WinAsyncClient(SOCKET socket, const EndPoint& addr);

            /**
             * Destructor.
             *
             * Should not be destructed from external threads.
             * Can be destructed from WorkerThread.
             */
            ~WinAsyncClient();

            /**
             * Shutdown client.
             *
             * Can be called from external threads.
             * Can be called from WorkerThread.
             */
            void Shutdown();

            /**
             * Wait for pending IO calls and wait till all IO are complete and reported.
             *
             * Can be called from external threads.
             * Can be called from WorkerThread.
             */
            void WaitForPendingIo();

            /**
             * Close client.
             *
             * Should not be called from external threads.
             * Can be called from WorkerThread.
             */
            void Close();

            /**
             * Add client to IOCP.
             *
             * @return IOCP handle on success and NULL otherwise.
             */
            HANDLE AddToIocp(HANDLE iocp);

            /**
             * Send packet using client.
             *
             * @param packet Packet to send.
             * @return @c true on success.
             */
            bool Send(const impl::interop::SP_InteropMemory& packet);

            /**
             * Initiate receiving of packet of the specified length. Received data is written to the receive buffer.
             *
             * @param bytes Packet size in bytes.
             * @return @c true on success.
             */
            bool Receive(size_t bytes);

            /**
             * Gets received data as a single buffer.
             * Clears client's receive buffer.
             *
             * @return Data received so far.
             */
            impl::interop::SP_InteropMemory DetachReceiveBuffer();

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
//                std::cout << "=============== WinAsyncClient: SetId " << id << ", " << socket << std::endl;

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
             * Set range.
             *
             * @param range Range.
             */
            void SetRange(const TcpRange& range)
            {
                this->range = range;
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
             * @return New packet if received completely or null.
             */
            impl::interop::SP_InteropMemory ProcessReceived(size_t bytes);

        private:
            /**
             * Send next packet in queue.
             *
             * @warning Can only be called when holding sendCs lock.
             * @return @c true on success.
             */
            bool SendNextPacketLocked();

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
            std::deque<impl::interop::SP_InteropMemory> sendPackets;

            /** Send critical section. */
            common::concurrent::CriticalSection sendCs;

            /** Current receive operation. */
            IoOperation currentRecv;

            /** Packet that is currently received. */
            impl::interop::SP_InteropMemory recvPacket;
        };

        /** Shared pointer to async client. */
        typedef common::concurrent::SharedPointer<WinAsyncClient> SP_WinAsyncClient;
    }
}

#endif //_IGNITE_NETWORK_WIN_ASYNC_CLIENT