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

#ifndef _IGNITE_ODBC_SOCKET_CLIENT
#define _IGNITE_ODBC_SOCKET_CLIENT

#include <stdint.h>

#include "ignite/odbc/diagnostic/diagnosable.h"

namespace ignite
{
    namespace odbc
    {
        /**
         * Socket client implementation.
         */
        class SocketClient
        {
        public:
            /** Connection establishment timeout in seconds. */
            enum { CONNECT_TIMEOUT = 5 };

            /**
             * Non-negative timeout operation result.
             */
            struct WaitResult
            {
                enum T
                {
                    /** Timeout. */
                    TIMEOUT = 0,

                    /** Success. */
                    SUCCESS = 1
                };
            };

            /**
             * Destructor.
             */
            virtual ~SocketClient()
            {
                // No-op.
            }

            /**
             * Establish connection with remote service.
             *
             * @param hostname Remote host name.
             * @param port Service port.
             * @param diag Diagnostics collector.
             * @return True on success.
             */
            virtual bool Connect(const char* hostname, uint16_t port, diagnostic::Diagnosable& diag) = 0;

            /**
             * Close established connection.
             *
             * @return True on success.
             */
            virtual void Close() = 0;

            /**
             * Send data by established connection.
             *
             * @param data Pointer to data to be sent.
             * @param size Size of the data in bytes.
             * @param timeout Timeout.
             * @return Number of bytes that have been sent on success, 
             *     WaitResult::TIMEOUT on timeout and -errno on failure.
             */
            virtual int Send(const int8_t* data, size_t size, int32_t timeout) = 0;

            /**
             * Receive data from established connection.
             *
             * @param buffer Pointer to data buffer.
             * @param size Size of the buffer in bytes.
             * @param timeout Timeout.
             * @return Number of bytes that have been received on success,
             *     WaitResult::TIMEOUT on timeout and -errno on failure.
             */
            virtual int Receive(int8_t* buffer, size_t size, int32_t timeout) = 0;

            /**
             * Check if the socket is blocking or not.
             * @return @c true if the socket is blocking and false otherwise.
             */
            virtual bool IsBlocking() const = 0;
        };
    }
}

#endif //_IGNITE_ODBC_SYSTEM_SOCKET_CLIENT