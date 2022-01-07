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

namespace ignite
{
    namespace network
    {
        WinAsyncClientPool::WinAsyncClientPool() :
            stopping(true),
            asyncHandler(0),
            connectingThread(),
            workerThread(),
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
                connectingThread.Start(*this, connLimit, addrs);
                workerThread.Start(*this, iocp);
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
                    std::cout << "=============== " << this << " " << " WinAsyncClientPool: Waiting for empty" << std::endl;

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
                    std::cout << "=============== " << this << " " << " WinAsyncClientPool: AddClient: stopping" << std::endl;

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
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnConnectionError(addr, err);
        }

        void WinAsyncClientPool::HandleConnectionSuccess(const EndPoint &addr, uint64_t id)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnConnectionSuccess(addr, id);
        }

        void WinAsyncClientPool::HandleConnectionClosed(uint64_t id, const IgniteError *err)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnConnectionClosed(id, err);
        }

        void WinAsyncClientPool::HandleMessageReceived(uint64_t id, const DataBuffer &msg)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnMessageReceived(id, msg);
        }

        void WinAsyncClientPool::HandleMessageSent(uint64_t id)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnMessageSent(id);
        }

        bool WinAsyncClientPool::Send(uint64_t id, const DataBuffer& data)
        {
            std::cout << "=============== " << this << " " << " Send: " << id << std::endl;
            if (stopping)
                return false;

            SP_WinAsyncClient client = FindClient(id);
            if (!client.IsValid())
                return false;

            std::cout << "=============== " << this << " " << " Send: Client found" << std::endl;
            return client.Get()->Send(data);
        }

        bool WinAsyncClientPool::CloseAndRelease(uint64_t id)
        {
            std::cout << "=============== " << this << " " << " WinAsyncClientPool: CloseAndRelease=" << id << std::endl;
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                std::map<uint64_t, SP_WinAsyncClient>::iterator it = clientIdMap.find(id);
                if (it == clientIdMap.end())
                    return false;

                client = it->second;

                clientIdMap.erase(it);

                std::cout << "=============== " << this << " " << " WorkerThread: clientIdMap.size=" << clientIdMap.size() << std::endl;
                if (clientIdMap.empty())
                {
                    std::cout << "=============== " << this << " " << " WorkerThread: Notifying about empty" << std::endl;
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

            std::cout << "=============== " << this << " " << " WinAsyncClientPool: CloseAndRelease, closed=" << closed << std::endl;
            std::cout << "=============== " << this << " " << " WinAsyncClientPool: CloseAndRelease, err=" << (err ? err->GetText() : "NULL") << std::endl;

            if (closed)
                HandleConnectionClosed(id, err);
        }

        bool WinAsyncClientPool::Close(uint64_t id)
        {
            std::cout << "=============== " << this << " " << " WinAsyncClientPool: Close=" << id << std::endl;

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

            std::cout << "=============== " << this << " " << " WinAsyncClientPool: Close, closed=" << closed << std::endl;
            std::cout << "=============== " << this << " " << " WinAsyncClientPool: Close, err=" << (err ? err->GetText() : "NULL") << std::endl;

            if (closed)
                HandleConnectionClosed(id, err);
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
