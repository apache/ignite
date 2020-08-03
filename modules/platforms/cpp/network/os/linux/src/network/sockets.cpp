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

                char buffer[1024] = "";

                if (!strerror_r(error, buffer, sizeof(buffer)))
                    res << ", msg=" << buffer;

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
        }
    }
}
