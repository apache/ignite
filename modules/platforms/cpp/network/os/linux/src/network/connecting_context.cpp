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
#include <sys/epoll.h>
#include <unistd.h>

#include <cstring>
#include <iterator>

#include <ignite/common/utils.h>
#include <ignite/network/utils.h>

#include "network/connecting_context.h"

namespace ignite
{
    namespace network
    {
        ConnectingContext::ConnectingContext(const TcpRange &range) :
            range(range),
            nextPort(range.port),
            info(0),
            currentInfo(0)
        {
            // No-op.
        }

        ConnectingContext::~ConnectingContext()
        {
            Reset();
        }

        void ConnectingContext::Reset()
        {
            if (info)
            {
                freeaddrinfo(info);
                info = 0;
                currentInfo = 0;
            }

            nextPort = range.port;
        }

        addrinfo *ConnectingContext::Next()
        {
            if (currentInfo)
                currentInfo = currentInfo->ai_next;

            while (!currentInfo)
            {
                if (info)
                {
                    freeaddrinfo(info);
                    info = 0;
                }

                if (nextPort > range.port + range.range)
                    return 0;

                addrinfo hints;
                std::memset(&hints, 0, sizeof(hints));

                hints.ai_family = AF_UNSPEC;
                hints.ai_socktype = SOCK_STREAM;
                hints.ai_protocol = IPPROTO_TCP;

                std::string strPort = common::LexicalCast<std::string>(nextPort);

                // Resolve the server address and port
                int res = getaddrinfo(range.host.c_str(), strPort.c_str(), &hints, &info);
                if (res != 0)
                    return 0;

                currentInfo = info;
                ++nextPort;
            }

            return currentInfo;
        }

        EndPoint ConnectingContext::GetAddress() const
        {
            return EndPoint(range.host, nextPort - 1);
        }

        SP_LinuxAsyncClient ConnectingContext::ToClient(int fd)
        {
            return SP_LinuxAsyncClient(new LinuxAsyncClient(fd, GetAddress(), range));
        }
    }
}
