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

#ifndef _IGNITE_ODBC_DRIVER_SOCKET_CLIENT
#define _IGNITE_ODBC_DRIVER_SOCKET_CLIENT

#define WIN32_LEAN_AND_MEAN
#define _WINSOCKAPI_

#include <windows.h>
#include <winsock2.h>

#include <stdint.h>

#include "ignite/common/common.h"

// Need to link with Ws2_32.lib, Mswsock.lib, and Advapi32.lib
#pragma comment (lib, "Ws2_32.lib")
#pragma comment (lib, "Mswsock.lib")
#pragma comment (lib, "AdvApi32.lib")

namespace ignite
{
    namespace odbc
    {
        /**
         * Initialize network.
         *
         * @return True on success.
         */
        bool InitNetworking();

        /**
         * Deinitialize network.
         */
        void DeinitNetworking();

        namespace tcp
        {
            /**
             * Socket client implementation.
             */
            class SocketClient
            {
            public:
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
                 * @return True on success.
                 */
                bool Connect(const char* hostname, uint16_t port);

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
                 * @return Number of bytes that have been sent on success and negative
                 *         value on failure.
                 */
                int Send(const uint8_t* data, size_t size);

                /**
                 * Receive data from established connection.
                 *
                 * @param data Pointer to data buffer.
                 * @param size Size of the buffer in bytes.
                 * @return Number of bytes that have been received on success and negative
                 *         value on failure.
                 */
                int Receive(uint8_t* buffer, size_t size);

            private:
                SOCKET socketHandle;

                IGNITE_NO_COPY_ASSIGNMENT(SocketClient)
            };
        }
    }
}

#endif