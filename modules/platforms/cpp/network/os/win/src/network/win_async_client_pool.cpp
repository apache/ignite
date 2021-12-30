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

                        clientPool.CloseAndRelease(client->GetId(), &err);
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
                            bool success = client->ProcessSent(bytesTransferred);

                            if (!success)
                            {
                                IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Can not send next packet");

                                clientPool.CloseAndRelease(client->GetId(), &err);
                            }

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
                    clientPool.CloseAndRelease(client->GetId(), &err);
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
            idGen(0),
            iocp(NULL),
            clientsCs(),
            clientIdMap()
        {
            // No-op.
        }

        WinAsyncClientPool::~WinAsyncClientPool()
        {
            InternalStop();
        }

        void WinAsyncClientPool::Start(const std::vector<TcpRange>& addrs, uint32_t connLimit)
        {
            if (!stopping)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Client pool is already started");

            stopping = false;

            sockets::InitWsa();

            iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
            if (iocp == NULL)
                ThrowSystemError("Failed to create IOCP instance");

            try
            {
                connectingThread.Start(connLimit, addrs);
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

        bool WinAsyncClientPool::AddClient(SP_WinAsyncClient& client)
        {
            uint64_t id;
            {
                WinAsyncClient& clientRef = *client.Get();

                common::concurrent::CsLockGuard lock(clientsCs);

                if (stopping)
                {
                    // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: AddClient: stopping" << std::endl;

                    return false;
                }

                id = ++idGen;
                clientRef.SetId(id);

                HANDLE iocp0 = clientRef.AddToIocp(iocp);
                if (iocp0 == NULL)
                    ThrowSystemError("Can not add socket to IOCP");

                iocp = iocp0;

                clientIdMap[id] = client;
            }

            PostQueuedCompletionStatus(iocp, 0, reinterpret_cast<ULONG_PTR>(client.Get()), 0);

            return true;
        }

        void WinAsyncClientPool::HandleConnectionError(const EndPoint &addr, const IgniteError &err)
        {
            if (asyncHandler)
                asyncHandler->OnConnectionError(addr, err);
        }

        bool WinAsyncClientPool::Send(uint64_t id, const DataBuffer& data)
        {
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " Send: " << id << std::endl;
            if (stopping)
                return false;

            SP_WinAsyncClient client = FindClient(id);
            if (!client.IsValid())
                return false;

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

                clientIdMap.erase(it);

                // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WorkerThread: clientIdMap.size=" << clientIdMap.size() << std::endl;
                if (clientIdMap.empty())
                {
                    // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WorkerThread: Notifying about empty" << std::endl;
                    clientsCv.NotifyAll();
                }
            }

            bool closed = client.Get()->Close();
            if (closed)
            {
                connectingThread.NotifyFreeAddress(client.Get()->GetRange());

                client.Get()->WaitForPendingIo();
            }

            return closed;
        }

        void WinAsyncClientPool::CloseAndRelease(uint64_t id, const IgniteError* err)
        {
            bool closed = CloseAndRelease(id);

            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: CloseAndRelease, closed=" << closed << std::endl;
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: CloseAndRelease, err=" << (err ? err->GetText() : "NULL") << std::endl;

            if (closed && asyncHandler)
                asyncHandler->OnConnectionClosed(id, err);
        }

        bool WinAsyncClientPool::Close(uint64_t id)
        {
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Close=" << id << std::endl;

            SP_WinAsyncClient client = FindClient(id);
            if (!client.IsValid() || client.Get()->IsClosed())
                return false;

            bool closed = client.Get()->Shutdown();

            if (closed)
                connectingThread.NotifyFreeAddress(client.Get()->GetRange());

            return closed;
        }

        void WinAsyncClientPool::Close(uint64_t id, const IgniteError* err)
        {
            bool closed = Close(id);

            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Close, closed=" << closed << std::endl;
            // std::cout << "=============== " << this << " " << GetCurrentThreadId() << " WinAsyncClientPool: Close, err=" << (err ? err->GetText() : "NULL") << std::endl;

            if (closed && asyncHandler)
                asyncHandler->OnConnectionClosed(id, err);
        }

        void WinAsyncClientPool::ThrowSystemError(const std::string& msg)
        {
            std::stringstream buf;

            buf << "Windows system error: " << msg << ", system error code: " << GetLastError();

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        SP_WinAsyncClient WinAsyncClientPool::FindClient(uint64_t id) const
        {
            common::concurrent::CsLockGuard lock(clientsCs);

            return FindClientLocked(id);
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
