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

#include <algorithm>

#include <ignite/network/utils.h>

#include "network/sockets.h"
#include "network/win_async_client_pool.h"

namespace ignite
{
    namespace network
    {
        WinAsyncClientPool::ConnectingThread::ConnectingThread(WinAsyncClientPool& clientPool) :
            clientPool(clientPool),
            stopping(false)
        {
            // No-op.
        }

        void WinAsyncClientPool::ConnectingThread::Run()
        {
            while (!stopping)
            {
                TcpRange range;

                {
                    common::concurrent::CsLockGuard lock(clientPool.clientsCs);

                    while (!clientPool.isConnectionNeeded())
                    {
                        connectNeeded.Wait(clientPool.clientsCs);

                        if (stopping)
                            return;
                    }

                    size_t idx = rand() % clientPool.nonConnected.size();
                    range = clientPool.nonConnected.at(idx);
                }

                SP_WinAsyncClient client = TryConnect(range);
                if (client.IsValid())
                {
                    try
                    {
                        uint64_t id = clientPool.AddClient(client);

                        clientPool.asyncHandler->OnConnectionSuccess(client.Get()->GetAddress(), id);
                    }
                    catch (const IgniteError& err)
                    {
                        client.Get()->Close();

                        clientPool.asyncHandler->OnConnectionError(client.Get()->GetAddress(), err);
                    }
                }
            }
        }

        void WinAsyncClientPool::ConnectingThread::WakeUp()
        {
            connectNeeded.NotifyOne();
        }

        void WinAsyncClientPool::ConnectingThread::Stop()
        {
            stopping = true;

            WakeUp();

            Join();
        }

        SP_WinAsyncClient WinAsyncClientPool::ConnectingThread::TryConnect(const TcpRange& range)
        {
            for (uint16_t port = range.port; port <= (range.port + range.range); ++port)
            {
                EndPoint addr(range.host, port);
                try
                {
                    SP_WinAsyncClient client = TryConnect(addr);
                    client.Get()->SetRange(range);

                    return client;
                }
                catch (const IgniteError& err)
                {
                    clientPool.asyncHandler->OnConnectionError(addr, err);
                }
            }

            return SP_WinAsyncClient();
        }

        SP_WinAsyncClient WinAsyncClientPool::ConnectingThread::TryConnect(const EndPoint& addr)
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

                enum { BUFFER_SIZE = 0x10000 };
                sockets::TrySetSocketOptions(socket, BUFFER_SIZE, TRUE, TRUE, TRUE);

                // Connect to server.
                res = WSAConnect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen), NULL, NULL, NULL, NULL);
                if (SOCKET_ERROR == res)
                {
                    closesocket(socket);

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

            return SP_WinAsyncClient(new WinAsyncClient(socket, addr));
        }

        WinAsyncClientPool::WorkerThread::WorkerThread(WinAsyncClientPool& clientPool) :
            clientPool(clientPool),
            stopping(false)
        {
            // No-op.
        }

        void WinAsyncClientPool::WorkerThread::Run()
        {
            while (!stopping)
            {
                DWORD bytesTransferred = 0;
                ULONG_PTR key = NULL;
                LPOVERLAPPED overLapped = NULL;

                BOOL ok = GetQueuedCompletionStatus(clientPool.iocp, &bytesTransferred, &key, &overLapped, INFINITE);

                if (!key || !overLapped)
                    continue;

                uint64_t id = static_cast<uint64_t>(key);

                if (!ok || 0 == bytesTransferred)
                {
                    IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed");
                    clientPool.Close(id, &err);
                    clientPool.CloseAndRelease(id);

                    continue;
                }

                // ...
            }
        }

        void WinAsyncClientPool::WorkerThread::Stop()
        {
            stopping = true;

            Join();
        }

        WinAsyncClientPool::WinAsyncClientPool() :
            asyncHandler(0),
            connectingThread(*this),
            workerThread(*this),
            idGen(0),
            clientIdMap(),
            clientAddrMap(),
            iocp(NULL)
        {
            // No-op.
        }

        WinAsyncClientPool::~WinAsyncClientPool()
        {
            InternalStop();
        }

        void WinAsyncClientPool::Start(const std::vector<TcpRange>& addrs, AsyncHandler& handler, uint32_t connLimit)
        {
            if (asyncHandler)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Client pool is already started");

            sockets::InitWsa();

            iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
            if (iocp == NULL)
                ThrowSystemError("Failed to create IOCP instance");

            nonConnected = addrs;
            asyncHandler = &handler;
            connectionLimit = connLimit;

            try
            {
                connectingThread.Start();
                workerThread.Start();
            }
            catch (...)
            {
                Stop();

                throw;
            }
        }

        void WinAsyncClientPool::Stop()
        {
            InternalStop();
        }

        void WinAsyncClientPool::InternalStop()
        {
            connectingThread.Stop();
            workerThread.Stop();

            nonConnected.clear();
            clientIdMap.clear();
            clientAddrMap.clear();

            CloseHandle(iocp);

            asyncHandler = 0;
            iocp = NULL;
        }

        bool WinAsyncClientPool::isConnectionNeeded() const
        {
            return (connectionLimit == 0 && !nonConnected.empty())
                   || (connectionLimit != 0 && connectionLimit > clientAddrMap.size());
        }

        uint64_t WinAsyncClientPool::AddClient(SP_WinAsyncClient& client)
        {
            uint64_t id;
            {
                WinAsyncClient& clientRef = *client.Get();

                common::concurrent::CsLockGuard lock(clientsCs);

                id = ++idGen;
                clientRef.SetId(id);

                HANDLE iocp0 = clientRef.AddToIocp(iocp);
                if (iocp0 == NULL)
                    ThrowSystemError("Can not add socket to IOCP");

                iocp = iocp0;

                clientIdMap[id] = client;
                clientAddrMap[clientRef.GetAddress()] = client;
                nonConnected.erase(std::find(nonConnected.begin(), nonConnected.end(), clientRef.GetRange()));
            }

            return id;
        }

        bool WinAsyncClientPool::Send(uint64_t id, impl::interop::SP_InteropMemory mem)
        {
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                client = LockedFindClient(id);
                if (!client.IsValid())
                    return false;
            }

            return client.Get()->Send(mem);
        }

        bool WinAsyncClientPool::CloseAndRelease(uint64_t id)
        {
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                std::map<uint64_t, SP_WinAsyncClient>::iterator it = clientIdMap.find(id);
                if (it == clientIdMap.end())
                    return false;

                client = it->second;

                if (!client.Get()->IsClosed())
                {
                    clientAddrMap.erase(client.Get()->GetAddress());
                    nonConnected.push_back(client.Get()->GetRange());

                    connectingThread.WakeUp();
                }

                clientIdMap.erase(it);
            }

            client.Get()->Close();
            client.Get()->WaitForPendingIo();

            return true;
        }

        bool WinAsyncClientPool::Close(uint64_t id)
        {
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                client = LockedFindClient(id);

                if (!client.IsValid() || client.Get()->IsClosed())
                    return false;

                clientAddrMap.erase(client.Get()->GetAddress());
                nonConnected.push_back(client.Get()->GetRange());

                // Leave client in clientIdMap until close event is received. Client instances contain OVERLAPPED
                // structures, which must not be freed until all async operations are complete.

                connectingThread.WakeUp();
            }

            client.Get()->Close();

            return true;
        }

        void WinAsyncClientPool::Close(uint64_t id, const IgniteError* err)
        {
            bool found = Close(id);

            if (found)
                asyncHandler->OnConnectionClosed(id, err);
        }

        void WinAsyncClientPool::ThrowSystemError(const std::string& msg)
        {
            std::stringstream buf;

            buf << "Windows system error: " << msg << ", system error code: " << GetLastError();

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        void WinAsyncClientPool::ThrowWsaError(const std::string& msg)
        {
            std::stringstream buf;

            buf << "WSA system error: " << msg << ", WSA error code: " << WSAGetLastError();

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        SP_WinAsyncClient WinAsyncClientPool::LockedFindClient(uint64_t id) const
        {
            std::map<uint64_t, SP_WinAsyncClient>::const_iterator it = clientIdMap.find(id);
            if (it == clientIdMap.end())
                return SP_WinAsyncClient();

            return it->second;
        }
    }
}
