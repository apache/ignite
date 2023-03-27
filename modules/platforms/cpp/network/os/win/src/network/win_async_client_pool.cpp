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
                common::ThrowLastSystemError("Failed to create IOCP instance");

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

                    client.Shutdown(0);
                    client.Close();
                }
            }

            workerThread.Stop();

            CloseHandle(iocp);
            iocp = NULL;

            clientIdMap.clear();
        }

        bool WinAsyncClientPool::AddClient(SP_WinAsyncClient& client)
        {
            uint64_t id;
            {
                WinAsyncClient& clientRef = *client.Get();

                common::concurrent::CsLockGuard lock(clientsCs);

                if (stopping)
                    return false;

                id = ++idGen;
                clientRef.SetId(id);

                HANDLE iocp0 = clientRef.AddToIocp(iocp);
                if (iocp0 == NULL)
                    common::ThrowLastSystemError("Can not add socket to IOCP");

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
            if (stopping)
                return false;

            SP_WinAsyncClient client = FindClient(id);
            if (!client.IsValid())
                return false;

            return client.Get()->Send(data);
        }

        void WinAsyncClientPool::CloseAndRelease(uint64_t id, const IgniteError* err)
        {
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                std::map<uint64_t, SP_WinAsyncClient>::iterator it = clientIdMap.find(id);
                if (it == clientIdMap.end())
                    return;

                client = it->second;

                clientIdMap.erase(it);
            }

            bool closed = client.Get()->Close();
            if (closed)
            {
                connectingThread.NotifyFreeAddress(client.Get()->GetRange());

                IgniteError err0(client.Get()->GetCloseError());
                if (err0.GetCode() == IgniteError::IGNITE_SUCCESS)
                    err0 = IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed by server");

                if (!err)
                    err = &err0;

                HandleConnectionClosed(id, err);
            }
        }

        void WinAsyncClientPool::Close(uint64_t id, const IgniteError* err)
        {
            SP_WinAsyncClient client = FindClient(id);
            if (client.IsValid() && !client.Get()->IsClosed())
                client.Get()->Shutdown(err);
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
