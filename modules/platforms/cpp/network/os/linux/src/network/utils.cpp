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

#include <cstdio>
#include <cstdlib>

#include <set>
#include <string>
#include <iostream>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <errno.h>

#include <ignite/ignite_error.h>

#include <ignite/network/utils.h>

namespace ignite
{
    namespace network
    {
        namespace utils
        {
            void GetLocalAddresses(std::set<std::string>& addrs)
            {
                struct ifaddrs *outAddrs;
                if(getifaddrs(&outAddrs) != 0)
                {
                    freeifaddrs(outAddrs);

                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Error getting local addresses list");
                }

                for (struct ifaddrs *outAddr = outAddrs; outAddr != NULL; outAddr = outAddr->ifa_next)
                {
                    if (outAddr->ifa_addr == NULL)
                        continue;

                    if (outAddr->ifa_flags & IFF_LOOPBACK)
                        continue;

                    if (!(outAddr->ifa_flags & IFF_UP))
                        continue;

                    void *inAddr;

                    char strBuffer[INET6_ADDRSTRLEN];

                    int saFamily = outAddr->ifa_addr->sa_family;

                    switch (saFamily)
                    {
                        case AF_INET:
                        {
                            sockaddr_in *s4 = reinterpret_cast<sockaddr_in*>(outAddr->ifa_addr);
                            inAddr = &s4->sin_addr;
                            break;
                        }

                        case AF_INET6:
                        {
                            sockaddr_in6 *s6 = reinterpret_cast<sockaddr_in6*>(outAddr->ifa_addr);
                            inAddr = &s6->sin6_addr;
                            break;
                        }

                        default:
                            continue;
                    }

                    inet_ntop(saFamily, inAddr, strBuffer, sizeof(strBuffer));

                    std::string strAddr(strBuffer);

                    if (!strAddr.empty())
                        addrs.insert(strAddr);
                }

                freeifaddrs(outAddrs);
            }
        }
    }
}

