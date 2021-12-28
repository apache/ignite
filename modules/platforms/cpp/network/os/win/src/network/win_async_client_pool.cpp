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

#include <ignite/common/utils.h>
#include <ignite/network/utils.h>

#include "network/sockets.h"
#include "network/win_async_client_pool.h"

namespace
{
    ignite::common::FibonacciSequence<10> fibonacci10;
}

namespace ignite
{
    namespace network
    {
        WinAsyncClientPool::ConnectingThread::ConnectingThread(WinAsyncClientPool& clientPool) :
            clientPool(clientPool),
            stopping(false),
            connectNeeded(),
            failedAttempts(0)
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

                    while (!clientPool.isConnectionNeededLocked())
                    {
                        connectNeeded.Wait(clientPool.clientsCs);

                        if (stopping)
                            return;
                    }

                    size_t idx = rand() % clientPool.nonConnected.size();
                    range = clientPool.nonConnected.at(idx);
                }

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

                uint64_t id = 0;

                try
                {
                    id = clientPool.AddClient(client);
                }
                catch (const IgniteError& err)
                {
                    client.Get()->Close();

                    if (clientPool.asyncHandler)
                        clientPool.asyncHandler->OnConnectionError(client.Get()->GetAddress(), err);

                    continue;
                }

                PostQueuedCompletionStatus(clientPool.iocp, 0, reinterpret_cast<ULONG_PTR>(client.Get()), 0);
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
                // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " ConnectingThread: port=" << port << std::endl;
                // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " ConnectingThread: range.port=" << range.port << std::endl;
                // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " ConnectingThread: range.range=" << range.range << std::endl;

                EndPoint addr(range.host, port);
                try
                {
                    SOCKET socket = TryConnect(addr);

                    return SP_WinAsyncClient(new WinAsyncClient(socket, addr, range, BUFFER_SIZE));
                }
                catch (const IgniteError& err)
                {
                    if (clientPool.asyncHandler)
                        clientPool.asyncHandler->OnConnectionError(addr, err);
                }
            }

            return SP_WinAsyncClient();
        }

        SOCKET WinAsyncClientPool::ConnectingThread::TryConnect(const EndPoint& addr)
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
                LPOVERLAPPED overlapped = NULL;

                BOOL ok = GetQueuedCompletionStatus(clientPool.iocp, &bytesTransferred, &key, &overlapped, INFINITE);

                // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: Got event" << std::endl;

                if (stopping)
                    break;

                if (!key)
                    continue;

                WinAsyncClient* client = reinterpret_cast<WinAsyncClient*>(key);

                if (!ok || (0 != overlapped && 0 == bytesTransferred))
                {
                    IoOperation* operation = reinterpret_cast<IoOperation*>(overlapped);
                    // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: closing " << client->GetId() << std::endl;
                    // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: bytesTransferred " << bytesTransferred << std::endl;
                    // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: operation=" << operation->kind << std::endl;

                    IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed");
                    clientPool.CloseAndRelease(client->GetId(), &err);

                    continue;
                }

                if (!overlapped)
                {
                    // This mean new client is connected.
                    if (clientPool.asyncHandler)
                        clientPool.asyncHandler->OnConnectionSuccess(client->GetAddress(), client->GetId());

                    // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: New connection. Initiating recv " << client->GetId() << std::endl;
                    bool success = client->Receive();
                    if (!success)
                    {
                        IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Can not initiate receiving of a first packet");

                        clientPool.Close(client->GetId(), &err);
                    }

                    continue;
                }

                try
                {
                    IoOperation* operation = reinterpret_cast<IoOperation*>(overlapped);
                    switch (operation->kind)
                    {
                        case IoOperationKind::SEND:
                        {
                            // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: processing send " << bytesTransferred << std::endl;
                            client->ProcessSent(bytesTransferred);

                            if (clientPool.asyncHandler)
                                clientPool.asyncHandler->OnMessageSent(client->GetId());

                            break;
                        }

                        case IoOperationKind::RECEIVE:
                        {
                            // std::cout << "=============== " << &clientPool << " " << GetCurrentThreadId() << " WorkerThread: processing recv " << bytesTransferred << std::endl;
                            DataBuffer data = client->ProcessReceived(bytesTransferred);

                            if (clientPool.asyncHandler && !data.IsEmpty())
                                clientPool.asyncHandler->OnMessageReceived(client->GetId(), data);

                            client->Receive();

                            break;
                        }

                        default:
                            break;
                    }
                }
                catch (const IgniteError& err)
                {
                    clientPool.Close(client->GetId(), &err);
                }
            }
        }

        void WinAsyncClientPool::WorkerThread::Stop()
        {
            stopping = true;

            PostQueuedCompletionStatus(clientPool.iocp, 0, 0, 0);

            Join();
        }

        WinAsyncClientPool::WinAsyncClientPool() :
            stopping(true),
            asyncHandler(0),
            connectingThread(*this),
            workerThread(*this),
            connectionLimit(0),
            idGen(0),
            iocp(NULL),
            clientsCs(),
            nonConnected(),
            clientIdMap(),
            clientAddrMap()
        {
            // No-op.
        }

        WinAsyncClientPool::~WinAsyncClientPool()
        {
            InternalStop();
        }

        void WinAsyncClientPool::Start(
            const std::vector<TcpRange>& addrs,
            uint32_t connLimit)
        {
            if (!stopping)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Client pool is already started");

            stopping = false;

            sockets::InitWsa();

            iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
            if (iocp == NULL)
                ThrowSystemError("Failed to create IOCP instance");

            nonConnected = addrs;
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
            stopping = true;
            asyncHandler = 0; // TODO: Remove this or make secure.
            connectingThread.Stop();

            {
                common::concurrent::CsLockGuard lock(clientsCs);
                nonConnected.clear();
                clientAddrMap.clear();

                std::map<uint64_t, SP_WinAsyncClient>::iterator it;
                for (it = clientIdMap.begin(); it != clientIdMap.end(); ++it)
                {
                    WinAsyncClient& client = *it->second.Get();

                    client.Shutdown();
                }

                if (!clientIdMap.empty())
                {
                    // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Waiting for empty" << std::endl;

                    clientsCv.Wait(clientsCs);
                }
            }

            workerThread.Stop();

            CloseHandle(iocp);
            iocp = NULL;
        }

        bool WinAsyncClientPool::isConnectionNeededLocked() const
        {
            return !nonConnected.empty() && (connectionLimit == 0 || connectionLimit > clientAddrMap.size());
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

        bool WinAsyncClientPool::Send(uint64_t id, const DataBuffer& data)
        {
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " Send: " << id << std::endl;
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                if (stopping)
                    return false;

                client = FindClientLocked(id);
                if (!client.IsValid())
                    return false;
            }

            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " Send: Client found" << std::endl;
            return client.Get()->Send(data);
        }

        bool WinAsyncClientPool::CloseAndRelease(uint64_t id)
        {
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: CloseAndRelease=" << id << std::endl;
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

                // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WorkerThread: clientIdMap.size=" << clientIdMap.size() << std::endl;
                if (clientIdMap.empty())
                {
                    // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WorkerThread: Notifying about empty" << std::endl;
                    clientsCv.NotifyAll();
                }
            }

            client.Get()->WaitForPendingIo();
            client.Get()->Close();

            return true;
        }

        void WinAsyncClientPool::CloseAndRelease(uint64_t id, const IgniteError* err)
        {
            bool found = CloseAndRelease(id);

            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: CloseAndRelease, found=" << found << std::endl;
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: CloseAndRelease, err=" << (err ? err->GetText() : "NULL") << std::endl;

            if (found && asyncHandler)
                asyncHandler->OnConnectionClosed(id, err);
        }

        bool WinAsyncClientPool::Close(uint64_t id)
        {
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Close=" << id << std::endl;
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                if (stopping)
                    return false;

                client = FindClientLocked(id);

                if (!client.IsValid() || client.Get()->IsClosed())
                    return false;

                clientAddrMap.erase(client.Get()->GetAddress());
                nonConnected.push_back(client.Get()->GetRange());

                // Leave client in clientIdMap until close event is received. Client instances contain OVERLAPPED
                // structures, which must not be freed until all async operations are complete.

                connectingThread.WakeUp();
            }

            client.Get()->Shutdown();

            return true;
        }

        void WinAsyncClientPool::Close(uint64_t id, const IgniteError* err)
        {
            bool found = Close(id);

            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Close, found=" << found << std::endl;
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Close, err=" << (err ? err->GetText() : "NULL") << std::endl;

            if (found && asyncHandler)
                asyncHandler->OnConnectionClosed(id, err);
        }

        void WinAsyncClientPool::ThrowSystemError(const std::string& msg)
        {
            std::stringstream buf;

            buf << "Windows system error: " << msg << ", system error code: " << GetLastError();

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        SP_WinAsyncClient WinAsyncClientPool::FindClientLocked(uint64_t id) const
        {
            std::map<uint64_t, SP_WinAsyncClient>::const_iterator it = clientIdMap.find(id);
            if (it == clientIdMap.end())
                return SP_WinAsyncClient();

            return it->second;
        }

        void WinAsyncClientPool::SetHandler(AsyncHandler *handler)
        {
            asyncHandler = handler;
        }
    }
}
