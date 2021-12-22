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

#include <ignite/common/concurrent.h>

#include <ignite/network/socket_client.h>
#include <ignite/network/utils.h>

namespace ignite
{
    namespace network
    {
        namespace sockets
        {
            int GetLastSocketError()
            {
                return WSAGetLastError();
            }

            int GetLastSocketError(int handle)
            {
                int lastError = 0;
                socklen_t size = sizeof(lastError);
                int res = getsockopt(handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

                return res == SOCKET_ERROR ? 0 : lastError;
            }

            std::string GetSocketErrorMessage(HRESULT error)
            {
                std::stringstream res;

                res << "error_code=" << error;

                if (error == 0)
                    return res.str();

                LPTSTR errorText = NULL;

                DWORD len = FormatMessageA(
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
                    reinterpret_cast<LPSTR>(&errorText),
                    // minimum size for output buffer
                    0,
                    // arguments - see note
                    NULL);

                if (NULL != errorText && len > 0)
                {
                    std::string msg(reinterpret_cast<const char*>(errorText), static_cast<size_t>(len));

                    if (len != 0)
                        res << ", msg=" << msg;

                    LocalFree(errorText);
                }

                return res.str();
            }

            std::string GetLastSocketErrorMessage()
            {
                HRESULT lastError = WSAGetLastError();

                return GetSocketErrorMessage(lastError);
            }

            int WaitOnSocket(SocketHandle socket, int32_t timeout, bool rd)
            {
                int ready = 0;
                int lastError = 0;

                fd_set fds;

                do {
                    struct timeval tv = { 0 };
                    tv.tv_sec = timeout;

                    FD_ZERO(&fds);
                    FD_SET(socket, &fds);

                    fd_set* readFds = 0;
                    fd_set* writeFds = 0;

                    if (rd)
                        readFds = &fds;
                    else
                        writeFds = &fds;

                    ready = select(static_cast<int>(socket) + 1, readFds, writeFds, NULL, timeout == 0 ? NULL : &tv);

                    if (ready == SOCKET_ERROR)
                        lastError = GetLastSocketError();

                } while (ready == SOCKET_ERROR && IsSocketOperationInterrupted(lastError));

                if (ready == SOCKET_ERROR)
                    return -lastError;

                if (ready == 0)
                    return SocketClient::WaitResult::TIMEOUT;

                return SocketClient::WaitResult::SUCCESS;
            }

            bool IsSocketOperationInterrupted(int errorCode)
            {
                return errorCode == WSAEINTR;
            }

            void TrySetSocketOptions(SOCKET socket, int bufSize, BOOL noDelay, BOOL outOfBand, BOOL keepAlive)
            {
                setsockopt(socket, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&bufSize), sizeof(bufSize));
                setsockopt(socket, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&bufSize), sizeof(bufSize));

                setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&noDelay), sizeof(noDelay));

                setsockopt(socket, SOL_SOCKET, SO_OOBINLINE, reinterpret_cast<char*>(&outOfBand), sizeof(outOfBand));

                int res = setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE,
                    reinterpret_cast<char*>(&keepAlive), sizeof(keepAlive));

                if (keepAlive)
                {
                    if (SOCKET_ERROR == res)
                    {
                        // There is no sense in configuring keep alive params if we failed to set up keep alive mode.
                        return;
                    }

                    // The time in seconds the connection needs to remain idle before starts sending keepalive probes.
                    enum { KEEP_ALIVE_IDLE_TIME = 60 };

                    // The time in seconds between individual keepalive probes.
                    enum { KEEP_ALIVE_PROBES_PERIOD = 1 };

#if defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL)
                    // This option is available starting with Windows 10, version 1709.
                    DWORD idleOpt = KEEP_ALIVE_IDLE_TIME;
                    DWORD idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;

                    setsockopt(socket, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));

                    setsockopt(socket, IPPROTO_TCP, TCP_KEEPINTVL,
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
                        socket,
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
            }

            bool SetNonBlockingMode(SOCKET socket, bool nonBlocking)
            {
                ULONG uTrueOpt = nonBlocking ? TRUE : FALSE;

                return (ioctlsocket(socket, FIONBIO, &uTrueOpt) != SOCKET_ERROR);
            }

            void InitWsa()
            {
                static common::concurrent::CriticalSection initCs;
                static bool networkInited = false;

                if (!networkInited)
                {
                    common::concurrent::CsLockGuard lock(initCs);
                    if (!networkInited)
                    {
                        WSADATA wsaData;

                        networkInited = WSAStartup(MAKEWORD(2, 2), &wsaData) == 0;

                        if (!networkInited)
                            utils::ThrowNetworkError("Networking initialisation failed: " + sockets::GetLastSocketErrorMessage());
                    }
                }
            }
        }
    }
}
