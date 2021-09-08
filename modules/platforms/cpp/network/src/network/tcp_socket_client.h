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

#ifndef _IGNITE_NETWORK_TCP_SOCKET_CLIENT
#define _IGNITE_NETWORK_TCP_SOCKET_CLIENT

#include "network/sockets.h"

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/network/socket_client.h>

namespace ignite
{
    namespace network
    {
        /**
         * Socket client implementation.
         */
        class TcpSocketClient : public SocketClient
        {
        public:
            /** Buffers size */
            enum { BUFFER_SIZE = 0x10000 };

            /** The time in seconds the connection needs to remain idle before starts sending keepalive probes. */
            enum { KEEP_ALIVE_IDLE_TIME = 60 };

            /** The time in seconds between individual keepalive probes. */
            enum { KEEP_ALIVE_PROBES_PERIOD = 1 };

            /**
             * Constructor.
             */
            TcpSocketClient();

            /**
             * Destructor.
             */
            virtual ~TcpSocketClient();

            /**
             * Establish connection with remote TCP service.
             *
             * @param hostname Remote host name.
             * @param port TCP service port.
             * @param timeout Timeout.
             * @return True on success.
             */
            virtual bool Connect(const char* hostname, uint16_t port, int32_t timeout);

            /**
             * Close established connection.
             */
            virtual void Close();

            /**
             * Send data by established connection.
             *
             * @param data Pointer to data to be sent.
             * @param size Size of the data in bytes.
             * @param timeout Timeout.
             * @return Number of bytes that have been sent on success,
             *     WaitResult::TIMEOUT on timeout and -errno on failure.
             */
            virtual int Send(const int8_t* data, size_t size, int32_t timeout);

            /**
             * Receive data from established connection.
             *
             * @param buffer Pointer to data buffer.
             * @param size Size of the buffer in bytes.
             * @param timeout Timeout.
             * @return Number of bytes that have been received on success,
             *     WaitResult::TIMEOUT on timeout and -errno on failure.
             */
            virtual int Receive(int8_t* buffer, size_t size, int32_t timeout);

            /**
             * Check if the socket is blocking or not.
             * @return @c true if the socket is blocking and false otherwise.
             */
            virtual bool IsBlocking() const;

        private:
            /**
             * Close established connection.
             */
            void InternalClose();

            /**
             * Tries set socket options.
             */
            void TrySetOptions();

            /**
             * Wait on the socket for any event for specified time.
             * This function uses poll to achive timeout functionality
             * for every separate socket operation.
             *
             * @param timeout Timeout.
             * @param rd Wait for read if @c true, or for write if @c false.
             * @return -errno on error, WaitResult::TIMEOUT on timeout and
             *     WaitResult::SUCCESS on success.
             */
            int WaitOnSocket(int32_t timeout, bool rd);

            /** Handle. */
            sockets::SocketHandle socketHandle;

            /** Blocking flag. */
            bool blocking;

            IGNITE_NO_COPY_ASSIGNMENT(TcpSocketClient);
        };
    }
}

#endif //_IGNITE_NETWORK_TCP_SOCKET_CLIENT
