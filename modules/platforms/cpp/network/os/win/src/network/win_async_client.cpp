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

#include <ignite/impl/binary/binary_utils.h>

#include "network/sockets.h"
#include "network/win_async_client.h"

namespace ignite
{
    namespace network
    {
        WinAsyncClient::WinAsyncClient(SOCKET socket, const EndPoint &addr) :
            state(State::CONNECTED),
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
            if (State::IN_POOL == state)
            {
                Shutdown();

                WaitForPendingIo();
            }

            if (State::CLOSED != state)
                Close();

            std::cout << "=============== ~WinAsyncClient " << id << std::endl;
        }

        void WinAsyncClient::Shutdown()
        {
            std::cout << "=============== WinAsyncClient::Shutdown " << id << std::endl;
            common::concurrent::CsLockGuard lock(sendCs);

            if (State::IN_POOL != state)
                return;

            shutdown(socket, SD_BOTH);

            CancelIo((HANDLE)socket);

            state = State::SHUTDOWN;
        }

        void WinAsyncClient::WaitForPendingIo()
        {
            std::cout << "=============== WinAsyncClient::WaitForPendingIo " << id << std::endl;
            while (!HasOverlappedIoCompleted(&currentSend.overlapped))
                GetOverlappedResult((HANDLE)socket, &currentSend.overlapped, NULL, TRUE);

            while (!HasOverlappedIoCompleted(&currentRecv.overlapped))
                GetOverlappedResult((HANDLE)socket, &currentRecv.overlapped, NULL, TRUE);
        }

        void WinAsyncClient::Close()
        {
            std::cout << "=============== WinAsyncClient::Close " << id << std::endl;
            closesocket(socket);

            sendPackets.clear();
            recvPacket = impl::interop::SP_InteropMemory();

            state = State::CLOSED;
        }

        HANDLE WinAsyncClient::AddToIocp(HANDLE iocp)
        {
            assert(State::CONNECTED == state);

            HANDLE res = CreateIoCompletionPort((HANDLE)socket, iocp, reinterpret_cast<DWORD_PTR>(this), 0);

            if (!res)
                return res;

            state = State::IN_POOL;

            return res;
        }

        bool WinAsyncClient::Send(const impl::interop::SP_InteropMemory& packet)
        {
            common::concurrent::CsLockGuard lock(sendCs);

            if (State::CONNECTED != state && State::IN_POOL != state)
                return false;

            sendPackets.push_back(packet);

            if (sendPackets.size() > 1)
                return true;

            return SendNextPacketLocked();
        }

        bool WinAsyncClient::SendNextPacketLocked()
        {
            if (sendPackets.empty())
                return true;

            const impl::interop::InteropMemory& packet0 = *sendPackets.front().Get();
            DWORD flags = 0;

            WSABUF buffer;
            buffer.buf = (CHAR*)packet0.Data();
            buffer.len = packet0.Length();

            currentSend.toTransfer = packet0.Length();
            currentSend.transferredSoFar = 0;

            std::cout << "=============== Send to " << id << " " << buffer.len << " bytes" << std::endl;
            int ret = WSASend(socket, &buffer, 1, NULL, flags, &currentSend.overlapped, NULL);

            return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
        }

        bool WinAsyncClient::Receive(size_t bytes)
        {
            // We do not need locking on receive as we're always reading in a single thread at most.
            if (State::CONNECTED != state && State::IN_POOL != state)
                return false;

            if (!recvPacket.IsValid())
                DetachReceiveBuffer();

            impl::interop::InteropMemory& packet0 = *recvPacket.Get();

            if (packet0.Capacity() < packet0.Length() + bytes)
                packet0.Reallocate(static_cast<int32_t>(packet0.Length() + bytes));

            DWORD flags = 0;
            WSABUF buffer;
            buffer.buf = (CHAR*)(packet0.Data() + currentRecv.transferredSoFar);
            buffer.len = (ULONG)bytes;

            std::cout << "=============== Recv from " << id << " " << buffer.len << " bytes" << std::endl;
            int ret = WSARecv(socket, &buffer, 1, NULL, &flags, &currentRecv.overlapped, NULL);

            return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
        }

        bool WinAsyncClient::ReceiveAll(size_t bytes)
        {
            if (!recvPacket.IsValid())
                DetachReceiveBuffer();

            if (currentRecv.toTransfer != currentRecv.transferredSoFar)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Internal error while receiving data from server: inconsistent buffer state");

            currentRecv.toTransfer += bytes;

            return Receive(bytes);
        }

        impl::interop::SP_InteropMemory WinAsyncClient::DetachReceiveBuffer()
        {
            using namespace impl::interop;

            // Allocating 4 KB for new packets
            SP_InteropMemory packet = SP_InteropMemory(new InteropUnpooledMemory(4 * 1024));

            packet.Swap(recvPacket);

            currentRecv.toTransfer = 0;
            currentRecv.transferredSoFar = 0;

            return packet;
        }

        impl::interop::SP_InteropMemory WinAsyncClient::ProcessReceived(size_t bytes)
        {
            std::cout << "=============== WinAsyncClient: currentRecv.transferredSoFar=" << currentRecv.transferredSoFar << std::endl;
            std::cout << "=============== WinAsyncClient: currentRecv.toTransfer=" << currentRecv.toTransfer << std::endl;

            currentRecv.transferredSoFar += bytes;

            impl::interop::InteropMemory& packet0 = *recvPacket.Get();

            packet0.Length(static_cast<int32_t>(currentRecv.transferredSoFar));

            if (currentRecv.transferredSoFar > currentRecv.toTransfer)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Internal error while receiving data from server: received more data than expected");

            if (currentRecv.transferredSoFar < currentRecv.toTransfer)
            {
                size_t left = currentRecv.toTransfer - currentRecv.transferredSoFar;
                Receive(left);

                return impl::interop::SP_InteropMemory();
            }

            if (packet0.Length() == IoOperation::PACKET_HEADER_SIZE)
            {
                int32_t msgLen = impl::binary::BinaryUtils::ReadInt32(packet0, 0);

                if (msgLen < 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Error while processing packet from server: received packet size is negative");

                ReceiveAll(static_cast<size_t>(msgLen));

                return impl::interop::SP_InteropMemory();
            }

            impl::interop::SP_InteropMemory packet = DetachReceiveBuffer();

            ReceiveAll(static_cast<size_t>(IoOperation::PACKET_HEADER_SIZE));

            return packet;
        }

        bool WinAsyncClient::ProcessSent(size_t bytes)
        {
            common::concurrent::CsLockGuard lock(sendCs);

            currentSend.transferredSoFar += bytes;

            if (currentSend.transferredSoFar > currentSend.toTransfer)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                    "Internal error while sending data to server: sent more data than expected");

            if (currentSend.transferredSoFar < currentSend.toTransfer)
                return true;

            sendPackets.pop_front();

            return SendNextPacketLocked();
        }
    }
}
