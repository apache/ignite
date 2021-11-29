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

#include <cstddef>

#include <ignite/common/fixed_size_array.h>
#include <ignite/network/network.h>

#include "impl/message.h"
#include "impl/remote_type_updater.h"
#include "impl/data_channel.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            const ProtocolVersion DataChannel::VERSION_1_2_0(1, 2, 0);
            const ProtocolVersion DataChannel::VERSION_1_3_0(1, 3, 0);
            const ProtocolVersion DataChannel::VERSION_1_4_0(1, 4, 0);
            const ProtocolVersion DataChannel::VERSION_1_5_0(1, 5, 0);
            const ProtocolVersion DataChannel::VERSION_1_6_0(1, 6, 0);
            const ProtocolVersion DataChannel::VERSION_1_7_0(1, 7, 0);
            const ProtocolVersion DataChannel::VERSION_DEFAULT(VERSION_1_7_0);

            DataChannel::VersionSet::value_type supportedArray[] = {
                DataChannel::VERSION_1_7_0,
                DataChannel::VERSION_1_6_0,
                DataChannel::VERSION_1_5_0,
                DataChannel::VERSION_1_4_0,
                DataChannel::VERSION_1_3_0,
                DataChannel::VERSION_1_2_0,
            };

            const DataChannel::VersionSet DataChannel::supportedVersions(supportedArray,
                supportedArray + (sizeof(supportedArray) / sizeof(supportedArray[0])));

            DataChannel::DataChannel(uint64_t id, network::SP_AsyncClientPool& asyncPool,
                const ignite::thin::IgniteClientConfiguration& cfg, binary::BinaryTypeManager& typeMgr,
                ChannelStateHandler& stateHandler) :
//                ioMutex(),
                stateHandler(stateHandler),
                handshakePerformed(false),
                id(id),
                asyncPool(asyncPool),
                node(),
                config(cfg),
                typeMgr(typeMgr),
                currentVersion(VERSION_DEFAULT),
                reqIdCounter(0)
            {
                // No-op.
            }

            DataChannel::~DataChannel()
            {
                Close();
            }

            void DataChannel::StartHandshake()
            {
                DoHandshake(VERSION_DEFAULT);
            }

            void DataChannel::Close()
            {
                asyncPool.Get()->Reset(id);
            }

            void DataChannel::AsyncMessage(const Request &req, Response &rsp, int32_t timeout)
            {
                // Allocating 64KB to lessen number of re-allocations.
                enum { BUFFER_SIZE = 1024 * 64 };

                interop::InteropUnpooledMemory mem(BUFFER_SIZE);

                int64_t id = GenerateRequestMessage(req, mem);

                InternalSyncMessage(mem, timeout);

                interop::InteropInputStream inStream(&mem);

                inStream.Position(4);

                int64_t rspId = inStream.ReadInt64();

                if (id != rspId)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Protocol error: Response message ID does not equal Request ID");

                binary::BinaryReaderImpl reader(&inStream);

                rsp.Read(reader, currentVersion);
            }

            int64_t DataChannel::GenerateRequestMessage(const Request &req, interop::InteropUnpooledMemory &mem)
            {
                interop::InteropOutputStream outStream(&mem);
                binary::BinaryWriterImpl writer(&outStream, &typeMgr);

                // Space for RequestSize + OperationCode + RequestID.
                outStream.Reserve(4 + 2 + 8);

                req.Write(writer, currentVersion);

                int64_t id = GenerateRequestId();

                outStream.WriteInt32(0, outStream.Position() - 4);
                outStream.WriteInt16(4, req.GetOperationCode());
                outStream.WriteInt64(6, id);

                outStream.Synchronize();

                return id;
            }

            void DataChannel::ProcessMessage(impl::interop::SP_InteropMemory msg)
            {
                if (!handshakePerformed)
                    OnHandshakeResponse(msg);

                // TODO: Implement me
            }

            void DataChannel::InternalSyncMessage(interop::InteropUnpooledMemory& mem, int32_t timeout)
            {
                common::concurrent::CsLockGuard lock(ioMutex);

                InternalSyncMessageUnguarded(mem, timeout);
            }

            void DataChannel::InternalSyncMessageUnguarded(interop::InteropUnpooledMemory& mem, int32_t timeout)
            {
                bool success = Send(mem.Data(), mem.Length(), timeout);

                if (!success)
                {
                    success = TryRestoreConnection(timeout);

                    if (!success)
                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                                          "Can not send message to remote host: timeout");

                    success = Send(mem.Data(), mem.Length(), timeout);

                    if (!success)
                        throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                                          "Can not send message to remote host: timeout");
                }

                success = Receive(mem, timeout);

                if (!success)
                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                                      "Can not receive message response from the remote host: timeout");
            }

            bool DataChannel::Send(const int8_t* data, size_t len, int32_t timeout)
            {
                if (socket.get() == 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "Connection is not established");

                OperationResult::T res = SendAll(data, len, timeout);

                if (res == OperationResult::TIMEOUT)
                    return false;

                if (res == OperationResult::FAIL)
                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                        "Can not send message due to connection failure");

                return true;
            }

            DataChannel::OperationResult::T DataChannel::SendAll(const int8_t* data, size_t len, int32_t timeout)
            {
                int sent = 0;

                while (sent != static_cast<int64_t>(len))
                {
                    int res = socket->Send(data + sent, len - sent, timeout);

                    if (res < 0 || res == network::SocketClient::WaitResult::TIMEOUT)
                    {
                        Close();

                        return res < 0 ? OperationResult::FAIL : OperationResult::TIMEOUT;
                    }

                    sent += res;
                }

                assert(static_cast<size_t>(sent) == len);

                return OperationResult::SUCCESS;
            }

            bool DataChannel::Receive(interop::InteropMemory& msg, int32_t timeout)
            {
                assert(msg.Capacity() > 4);

                if (socket.get() == 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "DataChannel is not established");

                // Message size
                msg.Length(4);

                OperationResult::T res = ReceiveAll(msg.Data(), static_cast<size_t>(msg.Length()), timeout);

                if (res == OperationResult::TIMEOUT)
                    return false;

                if (res == OperationResult::FAIL)
                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Can not receive message header");

                interop::InteropInputStream inStream(&msg);

                int32_t msgLen = inStream.ReadInt32();

                if (msgLen < 0)
                {
                    Close();

                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                        "Protocol error: Message length is negative");
                }

                if (msgLen == 0)
                    return true;

                if (msg.Capacity() < msgLen + 4)
                    msg.Reallocate(msgLen + 4);

                msg.Length(4 + msgLen);

                res = ReceiveAll(msg.Data() + 4, msgLen, timeout);

                if (res == OperationResult::TIMEOUT)
                    return false;

                if (res == OperationResult::FAIL)
                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE,
                        "Connection failure: Can not receive message body");

                return true;
            }

            DataChannel::OperationResult::T DataChannel::ReceiveAll(void* dst, size_t len, int32_t timeout)
            {
                size_t remain = len;
                int8_t* buffer = reinterpret_cast<int8_t*>(dst);

                while (remain)
                {
                    size_t received = len - remain;

                    int res = socket->Receive(buffer + received, remain, timeout);

                    if (res < 0 || res == network::SocketClient::WaitResult::TIMEOUT)
                    {
                        Close();

                        return res < 0 ? OperationResult::FAIL : OperationResult::TIMEOUT;
                    }

                    remain -= static_cast<size_t>(res);
                }

                return OperationResult::SUCCESS;
            }

            bool DataChannel::DoHandshake(const ProtocolVersion& propVer)
            {
                currentVersion = propVer;

                bool accepted;

                try
                {
                    // Workaround for some Linux systems that report connection on non-blocking
                    // sockets as successful but fail to establish real connection.
                    accepted = Handshake(propVer);
                }
                catch (const IgniteError&)
                {
                    return false;
                }

                return accepted;
            }

            bool DataChannel::Handshake(const ProtocolVersion& propVer)
            {
                // Allocating 4KB just in case.
                enum {
                    BUFFER_SIZE = 1024 * 4
                };

                interop::SP_InteropMemory mem(new interop::InteropUnpooledMemory(BUFFER_SIZE));
                interop::InteropOutputStream outStream(mem.Get());
                binary::BinaryWriterImpl writer(&outStream, 0);

                int32_t lenPos = outStream.Reserve(4);
                writer.WriteInt8(RequestType::HANDSHAKE);

                writer.WriteInt16(propVer.GetMajor());
                writer.WriteInt16(propVer.GetMinor());
                writer.WriteInt16(propVer.GetMaintenance());

                writer.WriteInt8(ClientType::THIN_CLIENT);

                if (propVer >= VERSION_1_7_0) {
                    // Use features for any new changes in protocol.
                    int8_t features[] = {0};

                    writer.WriteInt8Array(features, 0);
                }

                writer.WriteString(config.GetUser());
                writer.WriteString(config.GetPassword());

                outStream.WriteInt32(lenPos, outStream.Position() - 4);

                return asyncPool.Get()->Send(id, mem, config.GetConnectionTimeout());
            }

            void DataChannel::OnHandshakeResponse(impl::interop::SP_InteropMemory msg)
            {
                interop::InteropInputStream inStream(msg.Get());

                inStream.Position(4);

                binary::BinaryReaderImpl reader(&inStream);

                bool accepted = reader.ReadBool();

                if (!accepted)
                {
                    int16_t major = reader.ReadInt16();
                    int16_t minor = reader.ReadInt16();
                    int16_t maintenance = reader.ReadInt16();

                    ProtocolVersion resVer(major, minor, maintenance);

                    std::string error;
                    reader.ReadString(error);

                    int32_t errorCode = reader.ReadInt32();


                    bool shouldRetry = IsVersionSupported(resVer) && resVer != currentVersion;
                    if (shouldRetry)
                        shouldRetry = DoHandshake(resVer);

                    if (!shouldRetry)
                        SetChannelError(errorCode, error); // TODO: Implement me.

                    return;
                }

                if (currentVersion >= VERSION_1_7_0)
                {
                    int32_t len = reader.ReadInt8Array(0, 0);
                    std::vector<int8_t> features;

                    if (len > 0)
                    {
                        features.resize(static_cast<size_t>(len));
                        reader.ReadInt8Array(features.data(), len);
                    }
                }

                if (currentVersion >= VERSION_1_4_0)
                {
                    Guid nodeGuid = reader.ReadGuid();

                    node.SetGuid(nodeGuid);
                }

                stateHandler.OnHandshakeComplete(id);
            }

            bool DataChannel::IsVersionSupported(const ProtocolVersion& ver)
            {
                return supportedVersions.find(ver) != supportedVersions.end();
            }
        }
    }
}

