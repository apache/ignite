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
#include <winsock2.h>
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

            bool SocketClient::Connect(const char* hostname, uint16_t port, diagnostic::Diagnosable& diag)
            {
                static bool networkInited = false;

                // Initing networking if is not inited.
                if (!networkInited)
                {
                    WSADATA wsaData;

                    networkInited = (WSAStartup(MAKEWORD(2, 2), &wsaData) == 0);

                    if (!networkInited)
                    {
                        diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not initialize Windows networking.");

                        return false;
                    }
                }

                addrinfo hints;

                LOG_MSG("Host: " << hostname << " port: " << port);

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
                {
                    diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not resolve host address.");

                    return false;
                }

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
                    {
                        diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create new socket.");

                        return false;
                    }

                    diag.GetDiagnosticRecords().Reset();

                    TrySetOptions(diag);

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

            void SocketClient::TrySetOptions(diagnostic::Diagnosable& diag)
            {
                BOOL trueOpt = TRUE;
                int bufSizeOpt = BUFFER_SIZE;

                int res = setsockopt(socketHandle, SOL_SOCKET, SO_SNDBUF,
                    reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

                if (SOCKET_ERROR == res)
                {
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP socket send buffer size");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_RCVBUF,
                    reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

                if (SOCKET_ERROR == res)
                {
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP socket receive buffer size");
                }

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_NODELAY,
                    reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                if (SOCKET_ERROR == res)
                {
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP no-delay mode");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE,
                    reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                if (SOCKET_ERROR == res)
                {
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive mode");

                    // There is no sense in configuring keep alive params if we faileed to set up keep alive mode.
                    return;
                }

                // This option is available starting with Windows 10, version 1709.
#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL)
                DWORD idleOpt = KEEP_ALIVE_IDLE_TIME;
                DWORD idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPIDLE,
                    reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));

                if (SOCKET_ERROR == res)
                {
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive idle timeout");
                }

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL,
                    reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));

                if (SOCKET_ERROR == res)
                {
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive probes period");
                }
#else // use old hardcore WSAIoctl

                // WinSock structure for KeepAlive timing settings
                struct tcp_keepalive settings;
                settings.onoff = 1;
                settings.keepalivetime = KEEP_ALIVE_IDLE_TIME * 1000;
                settings.keepaliveinterval = KEEP_ALIVE_PROBES_PERIOD * 1000;

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
                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive idle timeout and probes period");
                }
#endif
            }

        }
    }
}

