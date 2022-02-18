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
#include "network/linux_async_client_pool.h"

namespace ignite
{
    namespace network
    {
        LinuxAsyncClientPool::LinuxAsyncClientPool() :
            stopping(true),
            asyncHandler(0),
            workerThread(*this),
            idGen(0),
            clientsCs(),
            clientIdMap()
        {
            // No-op.
        }

        LinuxAsyncClientPool::~LinuxAsyncClientPool()
        {
            InternalStop();
        }

        void LinuxAsyncClientPool::Start(const std::vector<TcpRange> &addrs, uint32_t connLimit)
        {
            if (!stopping)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Client pool is already started");

            idGen = 0;
            stopping = false;

            try
            {
                workerThread.Start0(connLimit, addrs);
            }
            catch (...)
            {
                Stop();

                throw;
            }
        }

        void LinuxAsyncClientPool::Stop()
        {
            InternalStop();
        }

        bool LinuxAsyncClientPool::Send(uint64_t id, const DataBuffer &data)
        {
            if (stopping)
                return false;

            SP_LinuxAsyncClient client = FindClient(id);
            if (!client.IsValid())
                return false;

            return client.Get()->Send(data);
        }

        void LinuxAsyncClientPool::Close(uint64_t id, const IgniteError *err)
        {
            if (stopping)
                return;

            SP_LinuxAsyncClient client = FindClient(id);
            if (client.IsValid() && !client.Get()->IsClosed())
                client.Get()->Shutdown(err);
        }

        void LinuxAsyncClientPool::CloseAndRelease(uint64_t id, const IgniteError *err)
        {
            if (stopping)
                return;

            SP_LinuxAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                std::map<uint64_t, SP_LinuxAsyncClient>::iterator it = clientIdMap.find(id);
                if (it == clientIdMap.end())
                    return;

                client = it->second;

                clientIdMap.erase(it);
            }

            bool closed = client.Get()->Close();
            if (closed)
            {
                IgniteError err0(client.Get()->GetCloseError());
                if (err0.GetCode() == IgniteError::IGNITE_SUCCESS)
                    err0 = IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed by server");

                if (!err)
                    err = &err0;

                HandleConnectionClosed(id, err);
            }
        }

        bool LinuxAsyncClientPool::AddClient(SP_LinuxAsyncClient &client)
        {
            if (stopping)
                return false;

            LinuxAsyncClient& clientRef = *client.Get();
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                uint64_t id = ++idGen;
                clientRef.SetId(id);

                clientIdMap[id] = client;
            }

            HandleConnectionSuccess(clientRef.GetAddress(), clientRef.GetId());

            return true;
        }

        void LinuxAsyncClientPool::HandleConnectionError(const EndPoint &addr, const IgniteError &err)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnConnectionError(addr, err);
        }

        void LinuxAsyncClientPool::HandleConnectionSuccess(const EndPoint &addr, uint64_t id)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnConnectionSuccess(addr, id);
        }

        void LinuxAsyncClientPool::HandleConnectionClosed(uint64_t id, const IgniteError *err)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnConnectionClosed(id, err);
        }

        void LinuxAsyncClientPool::HandleMessageReceived(uint64_t id, const DataBuffer &msg)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnMessageReceived(id, msg);
        }

        void LinuxAsyncClientPool::HandleMessageSent(uint64_t id)
        {
            AsyncHandler* asyncHandler0 = asyncHandler;
            if (asyncHandler0)
                asyncHandler0->OnMessageSent(id);
        }

        void LinuxAsyncClientPool::InternalStop()
        {
            stopping = true;
            workerThread.Stop();

            {
                common::concurrent::CsLockGuard lock(clientsCs);

                std::map<uint64_t, SP_LinuxAsyncClient>::iterator it;
                for (it = clientIdMap.begin(); it != clientIdMap.end(); ++it)
                {
                    LinuxAsyncClient& client = *it->second.Get();

                    IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Client stopped");
                    HandleConnectionClosed(client.GetId(), &err);
                }

                clientIdMap.clear();
            }
        }

        SP_LinuxAsyncClient LinuxAsyncClientPool::FindClient(uint64_t id) const
        {
            common::concurrent::CsLockGuard lock(clientsCs);

            std::map<uint64_t, SP_LinuxAsyncClient>::const_iterator it = clientIdMap.find(id);
            if (it == clientIdMap.end())
                return SP_LinuxAsyncClient();

            return it->second;
        }
    }
}
