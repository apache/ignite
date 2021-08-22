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
#include <netinet/tcp.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstring>

#include <sstream>

#include <ignite/common/concurrent.h>

#include <ignite/ignite_error.h>
#include "network/tcp_socket_client.h"

namespace ignite
{
    namespace network
    {
        TcpSocketClient::TcpSocketClient() :
            socketHandle(SOCKET_ERROR),
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
            addrinfo hints;

            std::memset(&hints, 0, sizeof(hints));

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

                if (socketHandle == SOCKET_ERROR)
                {
                    std::string err = "Socket creation failed: " + sockets::GetLastSocketErrorMessage();

                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, err.c_str());
                }

                TrySetOptions();

                // Connect to server.
                res = connect(socketHandle, it->ai_addr, static_cast<int>(it->ai_addrlen));
                if (SOCKET_ERROR == res)
                {
                    int lastError = errno;

                    if (lastError != EWOULDBLOCK && lastError != EINPROGRESS)
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

            if (socketHandle == SOCKET_ERROR)
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
            if (socketHandle != SOCKET_ERROR)
            {
                close(socketHandle);

                socketHandle = SOCKET_ERROR;
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
            int trueOpt = 1;

            int idleOpt = KEEP_ALIVE_IDLE_TIME;
            int idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;
            int bufSizeOpt = BUFFER_SIZE;

            setsockopt(socketHandle, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

            setsockopt(socketHandle, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

            setsockopt(socketHandle, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

            setsockopt(socketHandle, SOL_SOCKET, SO_OOBINLINE, reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

            blocking = false;

            int flags;
            blocking = ((flags = fcntl(socketHandle, F_GETFL, 0)) < 0) ||
                       (fcntl(socketHandle, F_SETFL, flags | O_NONBLOCK) < 0);

            int res = setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE,
                reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

            if (SOCKET_ERROR == res)
            {
                // There is no sense in configuring keep alive params if we faileed to set up keep alive mode.
                return;
            }
#ifdef __APPLE__
            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPALIVE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));
#else
            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));
#endif

            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL,
                reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));
        }

        int TcpSocketClient::WaitOnSocket(int32_t timeout, bool rd)
        {
            return sockets::WaitOnSocket(socketHandle, timeout, rd);
        }
    }
}

