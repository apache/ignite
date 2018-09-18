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

#include <sstream>

#include <ignite/ignite_error.h>

#include <ignite/common/concurrent.h>

#include "impl/net/tcp_socket_client.h"

namespace
{
    /**
     * Get socket error message for the error code.
     * @param error Error code.
     * @return Socket error message string.
     */
    std::string GetSocketErrorMessage(HRESULT error)
    {
        std::stringstream res;

        res << "error_code=" << error;

        if (error == 0)
            return res.str();

        LPTSTR errorText = NULL;

        DWORD len = FormatMessage(
            // use system message tables to retrieve error text
            FORMAT_MESSAGE_FROM_SYSTEM
            // allocate buffer on local heap for error text
            | FORMAT_MESSAGE_ALLOCATE_BUFFER
            // We're not passing insertion parameters
            | FORMAT_MESSAGE_IGNORE_INSERTS,
            // unused with FORMAT_MESSAGE_FROM_SYSTEM
            NULL,
            error,
            MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
            // output
            reinterpret_cast<LPTSTR>(&errorText),
            // minimum size for output buffer
            0,
            // arguments - see note
            NULL);

        if (NULL != errorText)
        {
            if (len != 0)
                res << ", msg=" << std::string(errorText, len);

            LocalFree(errorText);
        }

        return res.str();
    }

    /**
     * Get last socket error message.
     * @return Last socket error message string.
     */
    std::string GetLastSocketErrorMessage()
    {
        HRESULT lastError = WSAGetLastError();

        return GetSocketErrorMessage(lastError);
    }
}

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace net
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

                            networkInited = (WSAStartup(MAKEWORD(2, 2), &wsaData) == 0);

                            if (!networkInited)
                            {
                                std::string err = "Networking initialisation failed: " + GetLastSocketErrorMessage();

                                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, err.c_str());
                            }
                        }
                    }

                    addrinfo hints = { 0 };

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
                        // Create a SOCKET for connecting to server
                        socketHandle = socket(it->ai_family, it->ai_socktype, it->ai_protocol);

                        if (socketHandle == INVALID_SOCKET)
                        {
                            std::string err = "Socket creation failed: " + GetLastSocketErrorMessage();

                            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, err.c_str());
                        }

                        TrySetOptions();

                        // Connect to server.
                        res = connect(socketHandle, it->ai_addr, static_cast<int>(it->ai_addrlen));
                        if (SOCKET_ERROR == res)
                        {
                            int lastError = WSAGetLastError();

                            if (lastError != WSAEWOULDBLOCK)
                            {
                                Close();

                                continue;
                            }

                            res = WaitOnSocket(timeout, false);

                            if (res < 0 || res == WaitResult::TIMEOUT)
                            {
                                Close();

                                continue;
                            }
                        }
                        break;
                    }

                    freeaddrinfo(result);

                    return socketHandle != INVALID_SOCKET;
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


                    int res = setsockopt(socketHandle, SOL_SOCKET, SO_SNDBUF,
                        reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

                    if (SOCKET_ERROR == res)
                    {
//                        std::string err = "TCP socket send buffer size setup failed: " + GetLastSocketErrorMessage();
                    }

                    res = setsockopt(socketHandle, SOL_SOCKET, SO_RCVBUF,
                        reinterpret_cast<char*>(&bufSizeOpt), sizeof(bufSizeOpt));

                    if (SOCKET_ERROR == res)
                    {
//                        std::string err = "TCP socket receive buffer size setup failed: " + GetLastSocketErrorMessage();
                    }

                    res = setsockopt(socketHandle, IPPROTO_TCP, TCP_NODELAY,
                        reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                    if (SOCKET_ERROR == res)
                    {
//                        std::string err = "TCP no-delay mode setup failed: " + GetLastSocketErrorMessage();
                    }

                    res = setsockopt(socketHandle, SOL_SOCKET, SO_OOBINLINE,
                        reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                    if (SOCKET_ERROR == res)
                    {
//                        std::string err = "TCP out-of-bound data inlining setup failed: " + GetLastSocketErrorMessage();
                    }

                    blocking = false;
                    res = ioctlsocket(socketHandle, FIONBIO, &uTrueOpt);

                    if (res == SOCKET_ERROR)
                    {
                        blocking = true;
//                        std::string err = "Non-blocking mode setup failed: " + GetLastSocketErrorMessage();
                    }

                    res = setsockopt(socketHandle, SOL_SOCKET, SO_KEEPALIVE,
                        reinterpret_cast<char*>(&trueOpt), sizeof(trueOpt));

                    if (SOCKET_ERROR == res)
                    {
//                        std::string err = "TCP keep-alive mode setup failed: " + GetLastSocketErrorMessage();

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
//                        std::string err = "TCP keep-alive idle timeout setup failed: " + GetLastSocketErrorMessage();
                    }

                    res = setsockopt(socketHandle, IPPROTO_TCP, TCP_KEEPINTVL,
                        reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));

                    if (SOCKET_ERROR == res)
                    {
//                        std::string err = "TCP keep-alive probes period setup failed: " + GetLastSocketErrorMessage();
                    }
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
//                        std::string err = "TCP keep-alive params setup failed: " + GetLastSocketErrorMessage();
                    }
#endif
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

                    if (ready == 0)
                        return WaitResult::TIMEOUT;

                    return WaitResult::SUCCESS;
                }

                int TcpSocketClient::GetLastSocketError()
                {
                    return WSAGetLastError();
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
                    return errorCode == WSAEINTR;
                }
            }
        }
    }
}

