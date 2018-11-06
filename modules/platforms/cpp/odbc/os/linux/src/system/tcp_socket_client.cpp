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
#include <sys/poll.h>

#include <cstring>

#include <sstream>

#include "ignite/odbc/system/tcp_socket_client.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/log.h"

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
    namespace odbc
    {
        namespace system
        {

            TcpSocketClient::TcpSocketClient() :
                socketHandle(SOCKET_ERROR),
                blocking(true)
            {
                // No-op.
            }

            TcpSocketClient::~TcpSocketClient()
            {
                Close();
            }

            bool TcpSocketClient::Connect(const char* hostname, uint16_t port, int32_t timeout,
                diagnostic::Diagnosable& diag)
            {
                LOG_MSG("Host: " << hostname << ", port: " << port);

                addrinfo hints;

                memset(&hints, 0, sizeof(hints));
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
                {
                    LOG_MSG("Address resolving failed: " << gai_strerror(res));

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

                    if (socketHandle == SOCKET_ERROR)
                    {
                        LOG_MSG("Socket creation failed: " << GetLastSocketErrorMessage());

                        diag.AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "Can not create new socket.");

                        return false;
                    }

                    diag.GetDiagnosticRecords().Reset();

                    TrySetOptions(diag);

                    // Connect to server.
                    res = connect(socketHandle, it->ai_addr, static_cast<int>(it->ai_addrlen));
                    if (SOCKET_ERROR == res)
                    {
                        int lastError = errno;

                        if (lastError != EWOULDBLOCK && lastError != EINPROGRESS)
                        {
                            LOG_MSG("Connection failed: " << GetSocketErrorMessage(lastError));

                            Close();

                            continue;
                        }

                        res = WaitOnSocket(timeout == 0 ? -1 : timeout, false);

                        if (res < 0 || res == WaitResult::TIMEOUT)
                        {
                            LOG_MSG("Connection timeout expired: " << GetSocketErrorMessage(-res));

                            Close();

                            continue;
                        }
                    }
                    break;
                }

                freeaddrinfo(result);

                return socketHandle != SOCKET_ERROR;
            }

            void TcpSocketClient::Close()
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
                    int res = WaitOnSocket(timeout == 0 ? -1 : timeout, false);

                    if (res < 0 || res == WaitResult::TIMEOUT)
                        return res;
                }

                return send(socketHandle, reinterpret_cast<const char*>(data), static_cast<int>(size), 0);
            }

            int TcpSocketClient::Receive(int8_t* buffer, size_t size, int32_t timeout)
            {
                if (!blocking)
                {
                    int res = WaitOnSocket(timeout == 0 ? -1 : timeout, true);

                    if (res < 0 || res == WaitResult::TIMEOUT)
                        return res;
                }

                return recv(socketHandle, reinterpret_cast<char*>(buffer), static_cast<int>(size), 0);
            }

            bool TcpSocketClient::IsBlocking() const
            {
                return blocking;
            }

            void TcpSocketClient::TrySetOptions(diagnostic::Diagnosable& diag)
            {
                int trueOpt = 1;
                int bufSizeOpt = BUFFER_SIZE;
                int idleOpt = KEEP_ALIVE_IDLE_TIME;
                int idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;

                int res = setsockopt(socketHandle, SOL_SOCKET, SO_SNDBUF,
                    reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP socket send buffer size setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP socket send buffer size");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_RCVBUF,
                    reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP socket receive buffer size setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP socket receive buffer size");
                }

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_NODELAY,
                    reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP no-delay mode setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP no-delay mode");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_OOBINLINE,
                    reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP out-of-bound data inlining setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP out-of-bound data inlining");
                }

                blocking = false;

                int flags;
                if (((flags = fcntl(socketHandle, F_GETFL, 0)) < 0) ||
                    (fcntl(socketHandle, F_SETFL, flags | O_NONBLOCK) < 0))
                {
                    blocking = true;
                    LOG_MSG("Non-blocking mode setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up non-blocking mode. Timeouts are not available.");
                }

                res = setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE,
                    reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP keep-alive mode setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive mode");

                    // There is no sense in configuring keep alive params if we faileed to set up keep alive mode.
                    return;
                }

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPIDLE,
                    reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP keep-alive idle timeout setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive idle timeout");
                }

                res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL,
                    reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));

                if (SOCKET_ERROR == res)
                {
                    LOG_MSG("TCP keep-alive probes period setup failed: " << GetLastSocketErrorMessage());

                    diag.AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED,
                        "Can not set up TCP keep-alive probes period");
                }

            }

            int TcpSocketClient::WaitOnSocket(int32_t timeout, bool rd)
            {
                int lastError = 0;
                int ret;
                do
                {
                    struct pollfd fds[1];

                    fds[0].fd = socketHandle;
                    fds[0].events = rd ? POLLIN : POLLOUT;

                    ret = poll(fds, 1, timeout * 1000);

                    if (ret == SOCKET_ERROR)
                        lastError = GetLastSocketError();

                } while (ret == SOCKET_ERROR && IsSocketOperationInterrupted(lastError));

                if (ret == SOCKET_ERROR)
                    return -lastError;

                socklen_t size = sizeof(lastError);
                int res = getsockopt(socketHandle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

                if (res != SOCKET_ERROR && lastError != 0)
                    return -lastError;

                if (ret == 0)
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
}

