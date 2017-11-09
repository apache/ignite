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

#define WIN32_LEAN_AND_MEAN
#define _WINSOCKAPI_

#include <windows.h>
#include <ws2def.h>
#include <ws2tcpip.h>
#include <mstcpip.h>

#include <cstring>

#include <sstream>

#include "ignite/odbc/system/socket_client.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/log.h"

namespace ignite
{
    namespace odbc
    {
        namespace tcp
        {

            SocketClient::SocketClient() : socketHandle(INVALID_SOCKET)
            {
                // No-op.
            }

            SocketClient::~SocketClient()
            {
                Close();
            }

            bool SocketClient::Connect(const char* hostname, uint16_t port)
            {
                static bool networkInited = false;

                // Initing networking if is not inited.
                if (!networkInited)
                {
                    WSADATA wsaData;

                    networkInited = (WSAStartup(MAKEWORD(2, 2), &wsaData) == 0);

                    if (!networkInited)
                        return false;
                }

                addrinfo *result = NULL;
                addrinfo hints;

                LOG_MSG("Host: " << hostname << " port: " << port);

                memset(&hints, 0, sizeof(hints));
                hints.ai_family = AF_UNSPEC;
                hints.ai_socktype = SOCK_STREAM;
                hints.ai_protocol = IPPROTO_TCP;

                std::stringstream converter;
                converter << port;

                // Resolve the server address and port
                int res = getaddrinfo(hostname, converter.str().c_str(), &hints, &result);

                if (res != 0)
                    return false;

                // Attempt to connect to an address until one succeeds
                for (addrinfo *it = result; it != NULL; it = it->ai_next)
                {
                    LOG_MSG("Addr: " << (it->ai_addr->sa_data[2] & 0xFF) << "."
                                     << (it->ai_addr->sa_data[3] & 0xFF) << "."
                                     << (it->ai_addr->sa_data[4] & 0xFF) << "."
                                     << (it->ai_addr->sa_data[5] & 0xFF));

                    // Create a SOCKET for connecting to server
                    socketHandle = socket(it->ai_family, it->ai_socktype, it->ai_protocol);

                    if (socketHandle == INVALID_SOCKET)
                        return false;

                    if (!SetOptions(socketHandle))
                    {
                        Close();

                        return false;
                    }

                    // Connect to server.
                    res = connect(socketHandle, it->ai_addr, static_cast<int>(it->ai_addrlen));
                    if (SOCKET_ERROR == res)
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

            int SocketClient::Send(const int8_t* data, size_t size)
            {
                return send(socketHandle, reinterpret_cast<const char*>(data), static_cast<int>(size), 0);
            }

            int SocketClient::Receive(int8_t* buffer, size_t size)
            {
                return recv(socketHandle, reinterpret_cast<char*>(buffer), static_cast<int>(size), 0);
            }

            bool SocketClient::SetOptions(const intptr_t socketHandle)
            {
                bool trueOpt = true;
                int bufSizeOpt = IGNITE_SOCKET_SIZE;

                int res = setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE, (char *) &trueOpt, sizeof(trueOpt));
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("setsockopt failed for SO_KEEPALIVE");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_SNDBUF, (char *) &bufSizeOpt, sizeof(bufSizeOpt));
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("setsockopt failed for SO_SNDBUF");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_RCVBUF, (char *) &bufSizeOpt, sizeof(bufSizeOpt));
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("setsockopt failed for SO_RCVBUF");
                }
                
                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_NODELAY, (char *) &trueOpt, sizeof(trueOpt));
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("setsockopt failed for TCP_NODELAY");
                }

#ifdef defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL)	// This option is available starting with Windows 10, version 1709.
                DWORD idleOpt = IGNITE_TCP_KEEPIDLE;
                DWORD idleRetryOpt = IGNITE_TCP_KEEPINTVL;

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPIDLE, (char *) &idleOpt, sizeof(idleOpt));
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("setsockopt failed for TCP_KEEPIDLE");
                }

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL, (char *) &idleRetryOpt, sizeof(idleRetryOpt));
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("setsockopt failed for TCP_KEEPINTVL");
                }
#else // use old hardcore WSAIoctl

                // WinSock structure for KeepAlive timing settings
                struct tcp_keepalive settings;
                settings.onoff = 1;
                settings.keepalivetime = IGNITE_TCP_KEEPIDLE * 1000;
                settings.keepaliveinterval = IGNITE_TCP_KEEPINTVL * 1000;

                // pointers for WinSock call
                DWORD bytesReturned;
                WSAOVERLAPPED overlapped;
                overlapped.hEvent = NULL;

                // Set KeepAlive settings
                res = WSAIoctl(
                    socketHandle,
                    SIO_KEEPALIVE_VALS,
                    &settings,
                    sizeof(struct tcp_keepalive),
                    NULL,
                    0,
                    &bytesReturned,
                    &overlapped,
                    NULL
                );
                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("WSAIoctl failed for tcp_keepalive");
                }
#endif
                return true;
            }

        }
    }
}

