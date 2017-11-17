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

#ifndef _IGNITE_ODBC_SYSTEM_SOCKET_CLIENT
#define _IGNITE_ODBC_SYSTEM_SOCKET_CLIENT

#include <stdint.h>

#include "ignite/common/common.h"
#include "ignite/odbc/diagnostic/diagnosable.h"

namespace ignite
{
    namespace odbc
    {
        namespace tcp
        {
            /**
             * Socket client implementation.
             */
            class SocketClient
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
                SocketClient();

                /**
                 * Destructor.
                 */
                ~SocketClient();

                /**
                 * Establish connection with remote TCP service.
                 *
                 * @param hostname Remote host name.
                 * @param port TCP service port.
                 * @param diag Diagnostics collector.
                 * @return True on success.
                 */
                bool Connect(const char* hostname, uint16_t port, diagnostic::Diagnosable& diag);

                /**
                 * Close established connection.
                 *
                 * @return True on success.
                 */
                void Close();

                /**
                 * Send data by established connection.
                 *
                 * @param data Pointer to data to be sent.
                 * @param size Size of the data in bytes.
                 * @param timeout Timeout.
                 * @return Number of bytes that have been sent on success and negative
                 *         value or zero on failure.
                 */
                int Send(const int8_t* data, size_t size, int32_t timeout);

                /**
                 * Receive data from established connection.
                 *
                 * @param buffer Pointer to data buffer.
                 * @param size Size of the buffer in bytes.
                 * @param timeout Timeout.
                 * @return Number of bytes that have been received on success and negative
                 *         value or zero on failure.
                 */
                int Receive(int8_t* buffer, size_t size, int32_t timeout);

            private:
                /**
                 * Tries set socket options.
                 */
                void TrySetOptions(diagnostic::Diagnosable& diag);

                /**
                 * Wait on the socket for any event for specified time.
                 * @param timeout Timeout.
                 * @param rd Wait for read if @c true, or for write if @c false.
                 * @return -errno on error, zero on timeout and 1 on success.
                 */
                int WaitOnSocket(int32_t timeout, bool rd);

                /** Handle. */
                intptr_t socketHandle;

                /** Blocking flag. */
                bool blocking;

                IGNITE_NO_COPY_ASSIGNMENT(SocketClient)
            };
        }
    }
}

#endif //_IGNITE_ODBC_SYSTEM_SOCKET_CLIENT