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

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>

#include <cstring>

#include <sstream>

#include "ignite/odbc/system/socket_client.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/log.h"

#define SOCKET_ERROR (-1)

namespace ignite
{
    namespace odbc
    {
        namespace tcp
        {

            SocketClient::SocketClient() : socketHandle(SOCKET_ERROR)
            {
                // No-op.
            }

            SocketClient::~SocketClient()
            {
                Close();
            }

            bool SocketClient::Connect(const char* hostname, uint16_t port)
            {
                LOG_MSG("Host: " << hostname << ", port: " << port);

                addrinfo hints;
                memset(&hints, 0, sizeof(hints));
                hints.ai_family = AF_UNSPEC;
                hints.ai_socktype = SOCK_STREAM;
                hints.ai_protocol = IPPROTO_TCP;

                std::stringstream converter;
                converter << port;

                // Resolve the server address and port
                addrinfo *result = NULL;
                int res = getaddrinfo(hostname, converter.str().c_str(), &hints, &result);

                if (res != 0)
                    return false;

                // Attempt to connect to an address until one succeeds
                for (addrinfo *it = result; it != NULL; it = it->ai_next) 
                {
                    LOG_MSG("Addr: " << it->ai_addr->sa_data[2] << "."
                                     << it->ai_addr->sa_data[3] << "."
                                     << it->ai_addr->sa_data[4] << "."
                                     << it->ai_addr->sa_data[5]);

                    // Create a SOCKET for connecting to server
                    socketHandle = socket(it->ai_family, it->ai_socktype, it->ai_protocol);

                    if (socketHandle == SOCKET_ERROR)
                        return false;

                    // Connect to server.
                    res = connect(socketHandle, it->ai_addr, (int)it->ai_addrlen);
                    if (res == SOCKET_ERROR) 
                    {
                        Close();

                        continue;
                    }
                    break;
                }

                freeaddrinfo(result);

                return socketHandle != SOCKET_ERROR;
            }

            void SocketClient::Close()
            {
                if (socketHandle != SOCKET_ERROR)
                {
                    close(socketHandle);

                    socketHandle = SOCKET_ERROR;
                }
            }

            int SocketClient::Send(const int8_t* data, size_t size)
            {
                return send(socketHandle, reinterpret_cast<const char*>(data), static_cast<int>(size), 0);
            }

            int SocketClient::Receive(int8_t* buffer, size_t size)
            {
                return recv(socketHandle, reinterpret_cast<char*>(buffer), static_cast<int>(size), 0);
            }
        }
    }
}

