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

#include "network/sockets.h"

#include <sstream>

#include <ignite/ignite_error.h>

#include <ignite/common/concurrent.h>

#include "network/tcp_socket_client.h"

namespace ignite
{
    namespace network
    {
        TcpSocketClient::TcpSocketClient() :
            socketHandle(INVALID_SOCKET),
            blocking(true)
        {
            // No-op.
        }

        TcpSocketClient::~TcpSocketClient()
        {
            InternalClose();
        }

        bool TcpSocketClient::Connect(const char* hostname, uint16_t port, int32_t timeout)
        {
            static common::concurrent::CriticalSection initCs;
            static bool networkInited = false;

            // Initing networking if is not inited.
            if (!networkInited)
            {
                common::concurrent::CsLockGuard lock(initCs);
                if (!networkInited)
                {
                    WSADATA wsaData;

                    networkInited = WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;

                    if (!networkInited)
                        ThrowNetworkError("Networking initialisation failed: " + sockets::GetLastSocketErrorMessage());
                }
            }

            InternalClose();

            addrinfo hints = { 0 };

            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;

            std::stringstream converter;
            converter << port;
            std::string strPort = converter.str();

            // Resolve the server address and port
            addrinfo *result = NULL;
            int res = getaddrinfo(hostname, strPort.c_str(), &hints, &result);

            if (res != 0)
                ThrowNetworkError("Can not resolve host: " + std::string(hostname) + ":" + strPort);

            std::string lastErrorMsg = "Failed to resolve host";
            bool isTimeout = false;

            // Attempt to connect to an address until one succeeds
            for (addrinfo *it = result; it != NULL; it = it->ai_next)
            {
                lastErrorMsg = "Failed to establish connection with the host";
                isTimeout = false;

                // Create a SOCKET for connecting to server
                socketHandle = socket(it->ai_family, it->ai_socktype, it->ai_protocol);

                if (socketHandle == INVALID_SOCKET)
                    ThrowNetworkError("Socket creation failed: " + sockets::GetLastSocketErrorMessage());

                TrySetOptions();

                // Connect to server.
                res = connect(socketHandle, it->ai_addr, static_cast<int>(it->ai_addrlen));
                if (SOCKET_ERROR == res)
                {
                    int lastError = WSAGetLastError();

                    if (lastError != WSAEWOULDBLOCK)
                    {
                        lastErrorMsg.append(": ").append(sockets::GetSocketErrorMessage(lastError));

                        Close();

                        continue;
                    }

                    res = WaitOnSocket(timeout, false);

                    if (res < 0 || res == WaitResult::TIMEOUT)
                    {
                        isTimeout = true;

                        Close();

                        continue;
                    }
                }

                break;
            }

            freeaddrinfo(result);

            if (socketHandle == INVALID_SOCKET)
            {
                if (isTimeout)
                    return false;

                ThrowNetworkError(lastErrorMsg);
            }

            return true;
        }

        void TcpSocketClient::Close()
        {
            InternalClose();
        }

        void TcpSocketClient::InternalClose()
        {
            if (socketHandle != INVALID_SOCKET)
            {
                closesocket(socketHandle);

                socketHandle = INVALID_SOCKET;
            }
        }

        int TcpSocketClient::Send(const int8_t* data, size_t size, int32_t timeout)
        {
            if (!blocking)
            {
                int res = WaitOnSocket(timeout, false);

                if (res < 0 || res == WaitResult::TIMEOUT)
                    return res;
            }

            return send(socketHandle, reinterpret_cast<const char*>(data), static_cast<int>(size), 0);
        }

        int TcpSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
        {
            if (!blocking)
            {
                int res = WaitOnSocket(timeout, true);

                if (res < 0 || res == WaitResult::TIMEOUT)
                    return res;
            }

            return recv(socketHandle, reinterpret_cast<char*>(buffer), static_cast<int>(size), 0);
        }

        bool TcpSocketClient::IsBlocking() const
        {
            return blocking;
        }

        void TcpSocketClient::TrySetOptions()
        {
            BOOL trueOpt = TRUE;
            ULONG uTrueOpt = TRUE;
            int bufSizeOpt = BUFFER_SIZE;

            setsockopt(socketHandle, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

            setsockopt(socketHandle, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

            setsockopt(socketHandle, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

            setsockopt(socketHandle, SOL_SOCKET, SO_OOBINLINE, reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

            blocking = ioctlsocket(socketHandle, FIONBIO, &uTrueOpt) == SOCKET_ERROR;

            int res = setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE,
                reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

            if (SOCKET_ERROR == res)
            {
                // There is no sense in configuring keep alive params if we faileed to set up keep alive mode.
                return;
            }

            // This option is available starting with Windows 10, version 1709.
#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL)
            DWORD idleOpt = KEEP_ALIVE_IDLE_TIME;
            DWORD idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;

            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));

            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL,
                reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));
#else // use old hardcore WSAIoctl

            // WinSock structure for KeepAlive timing settings
            struct tcp_keepalive settings = { 0 };
            settings.onoff = 1;
            settings.keepalivetime = KEEP_ALIVE_IDLE_TIME * 1000;
            settings.keepaliveinterval = KEEP_ALIVE_PROBES_PERIOD * 1000;

            // pointers for WinSock call
            DWORD bytesReturned;
            WSAOVERLAPPED overlapped;
            overlapped.hEvent = NULL;

            // Set KeepAlive settings
            WSAIoctl(
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
#endif
        }

        int TcpSocketClient::WaitOnSocket(int32_t timeout, bool rd)
        {
            return sockets::WaitOnSocket(socketHandle, timeout, rd);
        }
    }
}

