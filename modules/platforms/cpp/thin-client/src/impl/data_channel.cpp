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
#include <ignite/common/promise.h>

#include <ignite/network/network.h>

#include "impl/message.h"
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

            DataChannel::DataChannel(uint64_t id, ignite::network::SP_AsyncClientPool asyncPool,
                const ignite::thin::IgniteClientConfiguration& cfg, binary::BinaryTypeManager& typeMgr,
                ChannelStateHandler& stateHandler) :
                stateHandler(stateHandler),
                handshakePerformed(false),
                id(id),
                asyncPool(asyncPool),
                node(),
                config(cfg),
                typeMgr(typeMgr),
                currentVersion(VERSION_DEFAULT),
                reqIdCounter(0),
                responseMutex()
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
                asyncPool.Get()->Close(id, 0);
            }

            void DataChannel::SyncMessage(const Request &req, Response &rsp, int32_t timeout)
            {
                Future<interop::SP_InteropMemory> rspFut = AsyncMessage(req, timeout);

                interop::SP_InteropMemory mem = rspFut.GetValue();

                interop::InteropInputStream inStream(mem.Get());

                // Skipping size and reqId
                inStream.Position(8);

                binary::BinaryReaderImpl reader(&inStream);

                rsp.Read(reader, currentVersion);
            }

            int64_t DataChannel::GenerateRequestMessage(const Request &req, interop::InteropMemory &mem)
            {
                interop::InteropOutputStream outStream(&mem);
                binary::BinaryWriterImpl writer(&outStream, &typeMgr);

                // Space for RequestSize + OperationCode + RequestID.
                outStream.Reserve(4 + 2 + 8);

                req.Write(writer, currentVersion);

                int64_t reqId = GenerateRequestId();

                outStream.WriteInt32(0, outStream.Position() - 4);
                outStream.WriteInt16(4, req.GetOperationCode());
                outStream.WriteInt64(6, reqId);

                outStream.Synchronize();

                return reqId;
            }

            Future<interop::SP_InteropMemory> DataChannel::AsyncMessage(const Request &req, int32_t timeout)
            {
                // Allocating 64 KB to decrease number of re-allocations.
                enum { BUFFER_SIZE = 1024 * 64 };

                interop::SP_InteropMemory mem(new interop::InteropUnpooledMemory(BUFFER_SIZE));

                int64_t reqId = GenerateRequestMessage(req, *mem.Get());

                common::Promise<interop::SP_InteropMemory> rsp;

                {
                    common::concurrent::CsLockGuard lock(responseMutex);

                    responseMap[reqId] = rsp;
                }

                bool success = asyncPool.Get()->Send(id, mem);

                if (!success)
                {
                    common::concurrent::CsLockGuard lock(responseMutex);

                    responseMap.erase(reqId);

                    std::string msg = "Can not send message to remote host " +
                        node.GetEndPoint().ToString() + ": timeout";

                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
                }

                return rsp.GetFuture();
            }

            void DataChannel::ProcessMessage(interop::SP_InteropMemory msg)
            {
                if (!handshakePerformed)
                {
                    OnHandshakeResponse(msg);

                    return;
                }

                interop::InteropInputStream inStream(msg.Get());

                inStream.Position(4);

                int64_t rspId = inStream.ReadInt64();
                int16_t flags = inStream.ReadInt16();

                if (flags & Flag::NOTIFICATION)
                {
                    common::concurrent::CsLockGuard lock(handlerMutex);

                    NotificationHandlerHolder& holder = handlerMap[rspId];
                    holder.ProcessNotification(msg);

                    if (holder.IsProcessingComplete())
                        handlerMap.erase(rspId);
                }
                else
                {
                    common::concurrent::CsLockGuard lock(responseMutex);

                    ResponseMap::iterator it = responseMap.find(rspId);

                    if (it != responseMap.end())
                    {
                        common::Promise<interop::SP_InteropMemory> rsp = it->second;

                        rsp.SetValue(msg);

                        responseMap.erase(rspId);
                    }
                }
            }

            void DataChannel::RegisterNotificationHandler(int64_t notId, const SP_NotificationHandler& handler)
            {
                common::concurrent::CsLockGuard lock(handlerMutex);

                NotificationHandlerHolder& holder = handlerMap[notId];
                holder.SetHandler(handler);

                if (holder.IsProcessingComplete())
                    handlerMap.erase(notId);
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
                // Allocating 4 KB just in case.
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

                return asyncPool.Get()->Send(id, mem);
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
                        SetChannelError(errorCode, error);

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

            void DataChannel::SetChannelError(int32_t code, const std::string& msg)
            {
                std::stringstream ss;
                ss << code << ": " << msg;
                std::string newMsg = ss.str();

                IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, newMsg.c_str());

                asyncPool.Get()->Close(id, &err);
            }
        }
    }
}

