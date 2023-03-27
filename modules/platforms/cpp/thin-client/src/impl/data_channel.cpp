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
            DataChannel::DataChannel(
                uint64_t id,
                const network::EndPoint& addr,
                const ignite::network::SP_AsyncClientPool& asyncPool,
                const ignite::thin::IgniteClientConfiguration& cfg,
                binary::BinaryTypeManager& typeMgr,
                ChannelStateHandler& stateHandler,
                common::ThreadPool& userThreadPool
            ) :
                stateHandler(stateHandler),
                handshakePerformed(false),
                id(id),
                asyncPool(asyncPool),
                node(addr),
                config(cfg),
                typeMgr(typeMgr),
                protocolContext(),
                reqIdCounter(0),
                responseMutex(),
                userThreadPool(userThreadPool)
            {
                // No-op.
            }

            DataChannel::~DataChannel()
            {
                Close(0);
            }

            void DataChannel::StartHandshake()
            {
                DoHandshake(ProtocolContext::VERSION_LATEST);
            }

            void DataChannel::Close(const IgniteError* err)
            {
                asyncPool.Get()->Close(id, err);
            }

            void DataChannel::SyncMessage(Request &req, Response &rsp, int32_t timeout)
            {
                Future<network::DataBuffer> rspFut = AsyncMessage(req);

                bool success = true;
                if (timeout)
                    success = rspFut.WaitFor(timeout);
                else
                    rspFut.Wait();

                if (!success)
                {
                    common::concurrent::CsLockGuard lock(responseMutex);

                    responseMap.erase(req.GetId());

                    std::string msg = "Can not send message to remote host " +
                        node.GetEndPoint().ToString() + " within timeout.";

                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
                }

                DeserializeMessage(rspFut.GetValue(), rsp);
            }

            int64_t DataChannel::GenerateRequestMessage(Request &req, interop::InteropMemory &mem)
            {
                interop::InteropOutputStream outStream(&mem);
                binary::BinaryWriterImpl writer(&outStream, &typeMgr);

                // Space for RequestSize + OperationCode + RequestID.
                outStream.Reserve(4 + 2 + 8);

                req.Write(writer, protocolContext);

                int64_t reqId = GenerateRequestId();
                req.SetId(reqId);

                outStream.WriteInt32(0, outStream.Position() - 4);
                outStream.WriteInt16(4, req.GetOperationCode());
                outStream.WriteInt64(6, reqId);

                outStream.Synchronize();

                return reqId;
            }

            Future<network::DataBuffer> DataChannel::AsyncMessage(Request &req)
            {
                // Allocating 64 KB to decrease number of re-allocations.
                enum { BUFFER_SIZE = 1024 * 64 };

                interop::SP_InteropMemory mem(new interop::InteropUnpooledMemory(BUFFER_SIZE));

                int64_t reqId = GenerateRequestMessage(req, *mem.Get());

                common::concurrent::CsLockGuard lock1(responseMutex);
                SP_PromiseDataBuffer& sp = responseMap[reqId];
                if (!sp.IsValid())
                    sp = SP_PromiseDataBuffer(new common::Promise<network::DataBuffer>());

                Future<network::DataBuffer> future = sp.Get()->GetFuture();
                lock1.Reset();

                network::DataBuffer buffer(mem);
                bool success = asyncPool.Get()->Send(id, buffer);

                if (!success)
                {
                    common::concurrent::CsLockGuard lock2(responseMutex);

                    responseMap.erase(reqId);

                    std::string msg = "Can not send message to remote host " + node.GetEndPoint().ToString();

                    throw IgniteError(IgniteError::IGNITE_ERR_NETWORK_FAILURE, msg.c_str());
                }

                return future;
            }

            void DataChannel::ProcessMessage(const network::DataBuffer& msg)
            {
                if (!handshakePerformed)
                {
                    OnHandshakeResponse(msg);
                    return;
                }

                interop::InteropInputStream inStream(msg.GetInputStream());

                inStream.Ignore(4);

                int64_t rspId = inStream.ReadInt64();
                int16_t flags = inStream.ReadInt16();

                if (flags & Flag::NOTIFICATION)
                {
                    common::SP_ThreadPoolTask task;
                    {
                        common::concurrent::CsLockGuard lock(handlerMutex);

                        NotificationHandlerHolder& holder = handlerMap[rspId];
                        task = holder.ProcessNotification(msg, id, stateHandler);
                    }

                    if (task.IsValid())
                        userThreadPool.Dispatch(task);
                }
                else
                {
                    common::concurrent::CsLockGuard lock(responseMutex);

                    ResponseMap::iterator it = responseMap.find(rspId);
                    if (it != responseMap.end())
                    {
                        common::Promise<network::DataBuffer>& rsp = *it->second.Get();

                        rsp.SetValue(std::auto_ptr<network::DataBuffer>(new network::DataBuffer(msg.Clone())));

                        responseMap.erase(rspId);
                    }
                }
            }

            void DataChannel::RegisterNotificationHandler(int64_t notId, const SP_NotificationHandler& handler)
            {
                common::concurrent::CsLockGuard lock(handlerMutex);

                NotificationHandlerHolder& holder = handlerMap[notId];
                holder.SetHandler(handler);
            }

            void DataChannel::DeregisterNotificationHandler(int64_t notId)
            {
                common::concurrent::CsLockGuard lock(handlerMutex);

                handlerMap.erase(notId);
            }

            bool DataChannel::DoHandshake(const ProtocolVersion& propVer)
            {
                ProtocolContext context(propVer);

                return Handshake(context);
            }

            bool DataChannel::Handshake(const ProtocolContext& context)
            {
                // Allocating 4 KB just in case.
                enum {
                    BUFFER_SIZE = 1024 * 4
                };

                interop::SP_InteropMemory mem(new interop::InteropUnpooledMemory(BUFFER_SIZE));
                interop::InteropOutputStream outStream(mem.Get());
                binary::BinaryWriterImpl writer(&outStream, 0);

                int32_t lenPos = outStream.Reserve(4);
                writer.WriteInt8(MessageType::HANDSHAKE);

                const ProtocolVersion& ver = context.GetVersion();

                writer.WriteInt16(ver.GetMajor());
                writer.WriteInt16(ver.GetMinor());
                writer.WriteInt16(ver.GetMaintenance());

                writer.WriteInt8(ClientType::THIN_CLIENT);

                if (context.IsFeatureSupported(VersionFeature::BITMAP_FEATURES))
                {
                    std::vector<int8_t> features = ProtocolContext::GetSupportedFeaturesMask();

                    writer.WriteInt8Array(&features[0], static_cast<int32_t>(features.size()));
                }

                writer.WriteString(config.GetUser());
                writer.WriteString(config.GetPassword());

                outStream.WriteInt32(lenPos, outStream.Position() - 4);

                outStream.Synchronize();

                network::DataBuffer buffer(mem);
                return asyncPool.Get()->Send(id, buffer);
            }

            void DataChannel::OnHandshakeResponse(const network::DataBuffer& msg)
            {
                interop::InteropInputStream inStream(msg.GetInputStream());

                inStream.Ignore(4);

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

                    bool shouldRetry = ProtocolContext::IsVersionSupported(resVer) &&
                        resVer != protocolContext.GetVersion();

                    if (shouldRetry)
                        shouldRetry = DoHandshake(resVer);

                    if (!shouldRetry)
                    {
                        std::stringstream ss;
                        ss << errorCode << ": " << error;
                        std::string newMsg = ss.str();

                        IgniteError err(IgniteError::IGNITE_ERR_GENERIC, newMsg.c_str());

                        if (!handshakePerformed)
                            stateHandler.OnHandshakeError(id, err);
                    }

                    return;
                }

                if (protocolContext.IsFeatureSupported(VersionFeature::BITMAP_FEATURES))
                {
                    int32_t len = reader.ReadInt8Array(0, 0);
                    std::vector<int8_t> features;

                    if (len > 0)
                    {
                        features.resize(static_cast<size_t>(len));
                        reader.ReadInt8Array(features.data(), len);
                    }

                    protocolContext.SetFeatures(features);
                }

                if (protocolContext.IsFeatureSupported(VersionFeature::PARTITION_AWARENESS))
                {
                    Guid nodeGuid = reader.ReadGuid();

                    node.SetGuid(nodeGuid);
                }

                handshakePerformed = true;
                stateHandler.OnHandshakeSuccess(id);
            }

            void DataChannel::DeserializeMessage(const network::DataBuffer &data, Response &msg)
            {
                interop::InteropInputStream inStream(data.GetInputStream());

                // Skipping size (4 bytes) and reqId (8 bytes)
                inStream.Ignore(12);

                binary::BinaryReaderImpl reader(&inStream);

                msg.Read(reader, protocolContext);
            }

            void DataChannel::FailPendingRequests(const IgniteError* err)
            {
                IgniteError defaultErr(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection was closed");
                if (!err)
                    err = &defaultErr;

                {
                    common::concurrent::CsLockGuard lock(responseMutex);

                    for (ResponseMap::iterator it = responseMap.begin(); it != responseMap.end(); ++it)
                        it->second.Get()->SetError(*err);

                    responseMap.clear();
                }

                {
                    common::concurrent::CsLockGuard lock(handlerMutex);

                    for (NotificationHandlerMap::iterator it = handlerMap.begin(); it != handlerMap.end(); ++it)
                    {
                        common::SP_ThreadPoolTask task = it->second.ProcessClosed();

                        if (task.IsValid())
                            userThreadPool.Dispatch(task);
                    }
                }

                if (!handshakePerformed)
                    stateHandler.OnHandshakeError(id, *err);
            }

            void DataChannel::CloseResource(int64_t resourceId)
            {
                ResourceCloseRequest req(resourceId);
                Response rsp;

                try
                {
                    SyncMessage(req, rsp, config.GetConnectionTimeout());
                }
                catch (const IgniteError& err)
                {
                    // Network failure means connection is closed or broken, which means
                    // that all resources were freed automatically.
                    if (err.GetCode() != IgniteError::IGNITE_ERR_NETWORK_FAILURE)
                        throw;
                }
            }
        }
    }
}

