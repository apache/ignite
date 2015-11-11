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

#include <ws2tcpip.h>

#include <cstring>

#include <sstream>

#include "socket_client.h"
#include "utility.h"

namespace ignite
{
    namespace odbc
    {

        bool InitNetworking()
        {
            WSADATA wsaData;

            return WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;
        }

        void DeinitNetworking()
        {
            WSACleanup();
        }

        namespace tcp
        {

            SocketClient::SocketClient() : socketHandle(INVALID_SOCKET)
            {
                // No-op.
            }

            SocketClient::~SocketClient()
            {
                // No-op.
            }

            bool SocketClient::Connect(const char* hostname, uint16_t port)
            {
                int iResult;
                addrinfo *result = NULL;
                addrinfo *ptr = NULL;
                addrinfo hints;

                LOG_MSG("Host: %s, port: %d\n", hostname, port);

                memset(&hints, 0, sizeof(hints));
                hints.ai_family = AF_UNSPEC;
                hints.ai_socktype = SOCK_STREAM;
                hints.ai_protocol = IPPROTO_TCP;

                std::stringstream converter;
                converter << port;

                // Resolve the server address and port
                iResult = getaddrinfo(hostname, converter.str().c_str(), &hints, &result);

                if (iResult != 0)
                    return false;

                // Attempt to connect to an address until one succeeds
                for (ptr = result; ptr != NULL; ptr = ptr->ai_next) 
                {
                    LOG_MSG("Addr: %u.%u.%u.%u\n", ptr->ai_addr->sa_data[2], ptr->ai_addr->sa_data[3], ptr->ai_addr->sa_data[4], ptr->ai_addr->sa_data[5]);

                    // Create a SOCKET for connecting to server
                    socketHandle = socket(ptr->ai_family, ptr->ai_socktype, ptr->ai_protocol);

                    if (socketHandle == INVALID_SOCKET)
                        return false;

                    // Connect to server.
                    iResult = connect(socketHandle, ptr->ai_addr, (int)ptr->ai_addrlen);
                    if (iResult == SOCKET_ERROR) 
                    {
                        Close();

                        continue;
                    }
                    break;
                }

                freeaddrinfo(result);

                return socketHandle != INVALID_SOCKET;
            }

            void SocketClient::Close()
            {
                if (socketHandle != INVALID_SOCKET)
                {
                    closesocket(socketHandle);

                    socketHandle = INVALID_SOCKET;
                }
            }

            int SocketClient::Send(const uint8_t * data, size_t size)
            {
                return send(socketHandle, reinterpret_cast<const char*>(data), static_cast<int>(size), 0);
            }
        }
    }
}

