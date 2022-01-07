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
#include "network/win_async_client.h"
#include "network/win_async_client_pool.h"
#include "network/win_async_worker_thread.h"

namespace
{
    ignite::common::FibonacciSequence<10> fibonacci10;
}

namespace ignite
{
    namespace network
    {
        WinAsyncWorkerThread::WinAsyncWorkerThread() :
            stopping(false),
            clientPool(0),
            iocp(NULL)
        {
            // No-op.
        }

        void WinAsyncWorkerThread::Start(WinAsyncClientPool& clientPool0, HANDLE iocp0)
        {
            assert(iocp0 != NULL);
            iocp = iocp0;
            clientPool = &clientPool0;

            Thread::Start();
        }

        void WinAsyncWorkerThread::Run()
        {
            assert(clientPool != 0);

            while (!stopping)
            {
                DWORD bytesTransferred = 0;
                ULONG_PTR key = NULL;
                LPOVERLAPPED overlapped = NULL;

                BOOL ok = GetQueuedCompletionStatus(iocp, &bytesTransferred, &key, &overlapped, INFINITE);

                // std::cout << "=============== " << clientPool << " " << " WorkerThread: Got event" << std::endl;

                if (stopping)
                    break;

                if (!key)
                    continue;

                WinAsyncClient* client = reinterpret_cast<WinAsyncClient*>(key);

                if (!ok || (0 != overlapped && 0 == bytesTransferred))
                {
                    IoOperation* operation = reinterpret_cast<IoOperation*>(overlapped);
                    // std::cout << "=============== " << clientPool << " " << " WorkerThread: closing " << client->GetId() << std::endl;
                    // std::cout << "=============== " << clientPool << " " << " WorkerThread: bytesTransferred " << bytesTransferred << std::endl;
                    // std::cout << "=============== " << clientPool << " " << " WorkerThread: operation=" << operation->kind << std::endl;

                    IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed");
                    clientPool->CloseAndRelease(client->GetId(), &err);

                    continue;
                }

                if (!overlapped)
                {
                    // This mean new client is connected.
                    clientPool->HandleConnectionSuccess(client->GetAddress(), client->GetId());

                    // std::cout << "=============== " << clientPool << " " << " WorkerThread: New connection. Initiating recv " << client->GetId() << std::endl;
                    bool success = client->Receive();
                    if (!success)
                    {
                        IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Can not initiate receiving of a first packet");

                        clientPool->CloseAndRelease(client->GetId(), &err);
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
                            // std::cout << "=============== " << clientPool << " " << " WorkerThread: processing send " << bytesTransferred << std::endl;
                            bool success = client->ProcessSent(bytesTransferred);

                            if (!success)
                            {
                                IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Can not send next packet");

                                clientPool->CloseAndRelease(client->GetId(), &err);
                            }

                            clientPool->HandleMessageSent(client->GetId());

                            break;
                        }

                        case IoOperationKind::RECEIVE:
                        {
                            // std::cout << "=============== " << clientPool << " " << " WorkerThread: processing recv " << bytesTransferred << std::endl;
                            DataBuffer data = client->ProcessReceived(bytesTransferred);

                            if (!data.IsEmpty())
                                clientPool->HandleMessageReceived(client->GetId(), data);

                            client->Receive();

                            break;
                        }

                        default:
                            break;
                    }
                }
                catch (const IgniteError& err)
                {
                    clientPool->CloseAndRelease(client->GetId(), &err);
                }
            }
        }

        void WinAsyncWorkerThread::Stop()
        {
            stopping = true;

            PostQueuedCompletionStatus(iocp, 0, 0, 0);

            Join();
        }
    }
}
