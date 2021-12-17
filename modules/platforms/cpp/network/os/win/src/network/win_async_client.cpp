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
#include "network/win_async_client.h"

namespace ignite
{
    namespace network
    {
        WinAsyncClient::WinAsyncClient(SOCKET socket, const EndPoint &addr) :
            socket(socket),
            id(0),
            addr(addr),
            range()
        {
            memset(&currentSend, 0, sizeof(currentSend));
            currentSend.kind = IoOperationKind::SEND;

            memset(&currentRecv, 0, sizeof(currentRecv));
            currentRecv.kind = IoOperationKind::RECEIVE;
        }

        WinAsyncClient::~WinAsyncClient()
        {
            Close();
            WaitForPendingIo();
        }

        void WinAsyncClient::Close()
        {
            if (socket)
            {
                closesocket(socket);
                socket = NULL;
                sendPackets.clear();
                recvPacket = impl::interop::SP_InteropMemory();
            }
        }

        void WinAsyncClient::WaitForPendingIo()
        {
            while (!HasOverlappedIoCompleted(&currentSend.overlapped))
                Sleep(0);

            while (!HasOverlappedIoCompleted(&currentRecv.overlapped))
                Sleep(0);
        }

        HANDLE WinAsyncClient::AddToIocp(HANDLE iocp)
        {
            return CreateIoCompletionPort((HANDLE)socket, iocp, reinterpret_cast<DWORD_PTR>(this), 0);
        }

        bool WinAsyncClient::Send(const impl::interop::SP_InteropMemory& packet)
        {
            common::concurrent::CsLockGuard lock(sendCs);

            sendPackets.push_back(packet);

            if (sendPackets.size() > 1)
                return true;

            const impl::interop::InteropMemory& packet0 = *packet.Get();
            DWORD sent = 0;
            DWORD flags = 0;

            WSABUF buffer;
            buffer.buf = (CHAR*)packet0.Data();
            buffer.len = packet0.Length();

            int ret = WSASend(socket, &buffer, 1, &sent, flags, &currentSend.overlapped, NULL);

            return !(ret == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()));
        }
    }
}
