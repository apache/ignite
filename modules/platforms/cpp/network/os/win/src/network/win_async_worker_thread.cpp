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

                if (stopping)
                    break;

                if (!key)
                    continue;

                WinAsyncClient* client = reinterpret_cast<WinAsyncClient*>(key);

                if (!ok || (0 != overlapped && 0 == bytesTransferred))
                {
                    clientPool->CloseAndRelease(client->GetId(), 0);

                    continue;
                }

                if (!overlapped)
                {
                    // This mean new client is connected.
                    clientPool->HandleConnectionSuccess(client->GetAddress(), client->GetId());

                    bool success = client->Receive();
                    if (!success)
                        clientPool->CloseAndRelease(client->GetId(), 0);

                    continue;
                }

                try
                {
                    IoOperation* operation = reinterpret_cast<IoOperation*>(overlapped);
                    switch (operation->kind)
                    {
                        case IoOperationKind::SEND:
                        {
                            bool success = client->ProcessSent(bytesTransferred);

                            if (!success)
                                clientPool->CloseAndRelease(client->GetId(), 0);

                            clientPool->HandleMessageSent(client->GetId());

                            break;
                        }

                        case IoOperationKind::RECEIVE:
                        {
                            DataBuffer data = client->ProcessReceived(bytesTransferred);

                            if (!data.IsEmpty())
                                clientPool->HandleMessageReceived(client->GetId(), data);

                            bool success = client->Receive();

                            if (!success)
                                clientPool->CloseAndRelease(client->GetId(), 0);

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
