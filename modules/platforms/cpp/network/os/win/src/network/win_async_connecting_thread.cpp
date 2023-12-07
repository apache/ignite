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

#include <ignite/common/utils.h>
#include <ignite/network/utils.h>

#include "network/sockets.h"
#include "network/win_async_client_pool.h"
#include "network/win_async_connecting_thread.h"

namespace
{
    ignite::common::FibonacciSequence<10> fibonacci10;
}

namespace ignite
{
    namespace network
    {
        WinAsyncConnectingThread::WinAsyncConnectingThread() :
            clientPool(0),
            stopping(false),
            failedAttempts(0),
            minAddrs(0),
            addrsCs(),
            connectNeeded(),
            nonConnected()
        {
            // No-op.
        }

        void WinAsyncConnectingThread::Run()
        {
            assert(clientPool != 0);

            while (!stopping)
            {
                TcpRange range = GetRandomAddress();

                if (stopping || range.IsEmpty())
                    break;

                SP_WinAsyncClient client = TryConnect(range);

                if (!client.IsValid())
                {
                    ++failedAttempts;

                    DWORD msToWait = static_cast<DWORD>(1000 * fibonacci10.GetValue(failedAttempts));
                    if (msToWait)
                        Sleep(msToWait);

                    continue;
                }

                failedAttempts = 0;

                if (stopping)
                {
                    client.Get()->Close();

                    return;
                }

                try
                {
                    bool added = clientPool->AddClient(client);

                    if (!added)
                    {
                        client.Get()->Close();

                        continue;
                    }

                    common::concurrent::CsLockGuard lock(addrsCs);
                    std::vector<TcpRange>::iterator it = std::find(nonConnected.begin(), nonConnected.end(), range);
                    if (it != nonConnected.end())
                        nonConnected.erase(it);
                }
                catch (const IgniteError& err)
                {
                    client.Get()->Close();

                    clientPool->HandleConnectionError(client.Get()->GetAddress(), err);

                    continue;
                }
            }
        }

        void WinAsyncConnectingThread::NotifyFreeAddress(const TcpRange &range)
        {
            common::concurrent::CsLockGuard lock(addrsCs);

            nonConnected.push_back(range);
            connectNeeded.NotifyOne();
        }

        void WinAsyncConnectingThread::Start(
            WinAsyncClientPool& clientPool0,
            size_t limit,
            const std::vector<TcpRange>& addrs)
        {
            stopping = false;
            clientPool = &clientPool0;
            failedAttempts = 0;
            nonConnected = addrs;

            if (!limit || limit > addrs.size())
                minAddrs = 0;
            else
                minAddrs = addrs.size() - limit;

            Thread::Start();
        }

        void WinAsyncConnectingThread::Stop()
        {
            stopping = true;

            {
                common::concurrent::CsLockGuard lock(addrsCs);
                connectNeeded.NotifyOne();
            }

            Join();
            nonConnected.clear();
        }

        SP_WinAsyncClient WinAsyncConnectingThread::TryConnect(const TcpRange& range)
        {
            for (uint16_t port = range.port; port <= (range.port + range.range); ++port)
            {
                EndPoint addr(range.host, port);
                try
                {
                    SOCKET socket = TryConnect(addr);

                    return SP_WinAsyncClient(new WinAsyncClient(socket, addr, range, BUFFER_SIZE));
                }
                catch (const IgniteError&)
                {
                    // No-op.
                }
            }

            return SP_WinAsyncClient();
        }

        SOCKET WinAsyncConnectingThread::TryConnect(const EndPoint& addr)
        {
            addrinfo hints;
            memset(&hints, 0, sizeof(hints));

            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;

            std::stringstream converter;
            converter << addr.port;
            std::string strPort = converter.str();

            // Resolve the server address and port
            addrinfo *result = NULL;
            int res = getaddrinfo(addr.host.c_str(), strPort.c_str(), &hints, &result);

            if (res != 0)
                utils::ThrowNetworkError("Can not resolve host: " + addr.host + ":" + strPort);

            std::string lastErrorMsg = "Failed to resolve host";

            SOCKET socket = INVALID_SOCKET;

            // Attempt to connect to an address until one succeeds
            for (addrinfo *it = result; it != NULL; it = it->ai_next)
            {
                lastErrorMsg = "Failed to establish connection with the host";

                socket = WSASocket(it->ai_family, it->ai_socktype, it->ai_protocol, NULL, 0, WSA_FLAG_OVERLAPPED);

                if (socket == INVALID_SOCKET)
                    utils::ThrowNetworkError("Socket creation failed: " + sockets::GetLastSocketErrorMessage());

                sockets::TrySetSocketOptions(socket, BUFFER_SIZE, TRUE, TRUE, TRUE);

                // Connect to server.
                res = WSAConnect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen), NULL, NULL, NULL, NULL);
                if (SOCKET_ERROR == res)
                {
                    closesocket(socket);
                    socket = INVALID_SOCKET;

                    int lastError = WSAGetLastError();

                    if (lastError != WSAEWOULDBLOCK)
                    {
                        lastErrorMsg.append(": ").append(sockets::GetSocketErrorMessage(lastError));

                        continue;
                    }
                }

                break;
            }

            freeaddrinfo(result);

            if (socket == INVALID_SOCKET)
                utils::ThrowNetworkError(lastErrorMsg);

            return socket;
        }

        TcpRange WinAsyncConnectingThread::GetRandomAddress() const
        {
            common::concurrent::CsLockGuard lock(addrsCs);

            if (stopping)
                return TcpRange();

            while (nonConnected.size() <= minAddrs)
            {
                connectNeeded.Wait(addrsCs);

                if (stopping)
                    return TcpRange();
            }

            size_t idx = rand() % nonConnected.size();
            TcpRange range = nonConnected.at(idx);

            lock.Reset();

            return range;
        }
    }
}
