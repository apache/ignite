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
#include <poll.h>

#include <errno.h>
#include <string.h>

#include <sstream>

#include "network/sockets.h"
#include <ignite/network/socket_client.h>

namespace ignite
{
    namespace network
    {
        namespace sockets
        {
            int GetLastSocketError()
            {
                return errno;
            }

            int GetLastSocketError(int handle)
            {
                int lastError = 0;
                socklen_t size = sizeof(lastError);
                int res = getsockopt(handle, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

                return res == SOCKET_ERROR ? 0 : lastError;
            }

            std::string GetSocketErrorMessage(int error)
            {
                std::stringstream res;

                res << "error_code=" << error;

                if (error == 0)
                    return res.str();

                char errBuf[1024] = { 0 };

                const char* errStr = strerror_r(error, errBuf, sizeof(errBuf));
                if (errStr)
                    res << ", msg=" << errStr;

                return res.str();
            }

            std::string GetLastSocketErrorMessage()
            {
                int lastError = errno;

                return GetSocketErrorMessage(lastError);
            }

            int WaitOnSocket(SocketHandle socket, int32_t timeout, bool rd)
            {
                int32_t timeout0 = timeout == 0 ? -1 : timeout;

                int lastError = 0;
                int ret;

                do
                {
                    struct pollfd fds[1];

                    fds[0].fd = socket;
                    fds[0].events = rd ? POLLIN : POLLOUT;

                    ret = poll(fds, 1, timeout0 * 1000);

                    if (ret == SOCKET_ERROR)
                        lastError = GetLastSocketError();

                } while (ret == SOCKET_ERROR && IsSocketOperationInterrupted(lastError));

                if (ret == SOCKET_ERROR)
                    return -lastError;

                socklen_t size = sizeof(lastError);
                int res = getsockopt(socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&lastError), &size);

                if (res != SOCKET_ERROR && lastError != 0)
                    return -lastError;

                if (ret == 0)
                    return SocketClient::WaitResult::TIMEOUT;

                return SocketClient::WaitResult::SUCCESS;
            }

            bool IsSocketOperationInterrupted(int errorCode)
            {
                return errorCode == EINTR;
            }

            void TrySetSocketOptions(int socketFd, int bufSize, bool noDelay, bool outOfBand, bool keepAlive)
            {
                setsockopt(socketFd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&bufSize), sizeof(bufSize));
                setsockopt(socketFd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&bufSize), sizeof(bufSize));

                int iNoDelay = noDelay ? 1 : 0;
                setsockopt(socketFd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&iNoDelay), sizeof(iNoDelay));

                int iOutOfBand = outOfBand ? 1 : 0;
                setsockopt(socketFd, SOL_SOCKET, SO_OOBINLINE,
                    reinterpret_cast<char*>(&iOutOfBand), sizeof(iOutOfBand));

                int iKeepAlive = keepAlive ? 1 : 0;
                int res = setsockopt(socketFd, SOL_SOCKET, SO_KEEPALIVE,
                    reinterpret_cast<char*>(&iKeepAlive), sizeof(iKeepAlive));

                if (SOCKET_ERROR == res)
                {
                    // There is no sense in configuring keep alive params if we faileed to set up keep alive mode.
                    return;
                }

                // The time in seconds the connection needs to remain idle before starts sending keepalive probes.
                enum { KEEP_ALIVE_IDLE_TIME = 60 };

                // The time in seconds between individual keepalive probes.
                enum { KEEP_ALIVE_PROBES_PERIOD = 1 };

                int idleOpt = KEEP_ALIVE_IDLE_TIME;
                int idleRetryOpt = KEEP_ALIVE_PROBES_PERIOD;
#ifdef __APPLE__
                setsockopt(socketFd, IPPROTO_TCP, TCP_KEEPALIVE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));
#else
                setsockopt(socketFd, IPPROTO_TCP, TCP_KEEPIDLE, reinterpret_cast<char*>(&idleOpt), sizeof(idleOpt));
#endif

                setsockopt(socketFd, IPPROTO_TCP, TCP_KEEPINTVL,
                    reinterpret_cast<char*>(&idleRetryOpt), sizeof(idleRetryOpt));
            }

            bool SetNonBlockingMode(int socketFd, bool nonBlocking)
            {
                int flags = fcntl(socketFd, F_GETFL, 0);
                if (flags == -1)
                    return false;

                bool currentNonBlocking = flags & O_NONBLOCK;
                if (nonBlocking == currentNonBlocking)
                    return true;

                flags ^= O_NONBLOCK;
                int res = fcntl(socketFd, F_SETFL, flags);

                return res != -1;
            }
        }
    }
}
