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

#define SOCKET_ERROR (-1)

namespace
{
    /**
     * Get last socket error message.
     * @param error Error code.
     * @return Last socket error message string.
     */
    std::string GetSocketErrorMessage(int error)
    {
        std::stringstream res;

        res << "error_code=" << error;

        if (error == 0)
            return res.str();

        char buffer[1024] = "";

        if (!strerror_r(error, buffer, sizeof(buffer)))
            res << ", msg=" << buffer;

        return res.str();
    }

    /**
     * Get last socket error message.
     * @return Last socket error message string.
     */
    std::string GetLastSocketErrorMessage()
    {
        int lastError = errno;

        return GetSocketErrorMessage(lastError);
    }
}

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

                if (socketHandle == SOCKET_ERROR)
                {
                    std::string err = "Socket creation failed: " + GetLastSocketErrorMessage();

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
                        lastErrorMsg.append(": ").append(GetSocketErrorMessage(lastError));

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

            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));

            setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL,
                reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));
        }

        int TcpSocketClient::WaitOnSocket(int32_t timeout, bool rd)
        {
            int ready = 0;
            int lastError = 0;

            fd_set fds;

            do {
                struct timeval tv = { 0 };
                tv.tv_sec = timeout;

                FD_ZERO(&fds);
                FD_SET(socketHandle, &fds);

                fd_set* readFds = 0;
                fd_set* writeFds = 0;

                if (rd)
                    readFds = &fds;
                else
                    writeFds = &fds;

                ready = select(static_cast<int>((socketHandle) + 1),
                    readFds, writeFds, NULL, (timeout == 0 ? NULL : &tv));

                if (ready == SOCKET_ERROR)
                    lastError = GetLastSocketError();

            } while (ready == SOCKET_ERROR && IsSocketOperationInterrupted(lastError));

            if (ready == SOCKET_ERROR)
                return -lastError;

            socklen_t size = sizeof(lastError);
            int res = getsockopt(socketHandle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

            if (res != SOCKET_ERROR && lastError != 0)
                return -lastError;

            if (ready == 0)
                return WaitResult::TIMEOUT;

            return WaitResult::SUCCESS;
        }

        int TcpSocketClient::GetLastSocketError()
        {
            return errno;
        }

        int TcpSocketClient::GetLastSocketError(int handle)
        {
            int lastError = 0;
            socklen_t size = sizeof(lastError);
            int res = getsockopt(handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

            return res == SOCKET_ERROR ? 0 : lastError;
        }

        bool TcpSocketClient::IsSocketOperationInterrupted(int errorCode)
        {
            return errorCode == EINTR;
        }
    }
}

