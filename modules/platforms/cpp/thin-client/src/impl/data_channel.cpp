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

#include <cstring>
#include <cstddef>

#include <sstream>

#include <ignite/common/fixed_size_array.h>

#include "impl/message.h"
#include "impl/ssl/ssl_gateway.h"
#include "impl/ssl/secure_socket_client.h"
#include "impl/net/tcp_socket_client.h"
#include "impl/net/remote_type_updater.h"

#include "impl/data_channel.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** Version 1.1.0. */
            const ProtocolVersion DataChannel::VERSION_1_1_0(1, 1, 0);

            /** Current version. */
            const ProtocolVersion DataChannel::VERSION_CURRENT(VERSION_1_1_0);

            DataChannel::VersionSet::value_type supportedArray[] = {
                DataChannel::VERSION_1_1_0,
            };

            const DataChannel::VersionSet DataChannel::supportedVersions(supportedArray,
                supportedArray + (sizeof(supportedArray) / sizeof(supportedArray[0])));

            DataChannel::DataChannel(const ignite::thin::IgniteClientConfiguration& cfg,
                binary::BinaryTypeManager& typeMgr) :
                ioMutex(),
                address(),
                config(cfg),
                typeMgr(typeMgr),
                currentVersion(VERSION_CURRENT),
                reqIdCounter(0),
                socket()
            {
                // No-op.
            }

            DataChannel::~DataChannel()
            {
                // No-op.
            }

            bool DataChannel::Connect(const std::string& host, uint16_t port, int32_t timeout)
            {
                using ignite::thin::SslMode;

                if (socket.get() != 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "Already connected.");

                if (config.GetEndPoints().empty())
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, "No valid address to connect.");

                SslMode::Type sslMode = config.GetSslMode();

                if (sslMode != SslMode::DISABLE)
                {
                    ssl::SslGateway::GetInstance().LoadAll();

                    socket.reset(new ssl::SecureSocketClient(config.GetSslCertFile(),
                        config.GetSslKeyFile(), config.GetSslCaFile()));
                }
                else
                    socket.reset(new net::TcpSocketClient());

                address.host = host;
                address.port = port;

                return TryRestoreConnection(timeout);
            }

            void DataChannel::Close()
            {
                if (socket.get() != 0)
                {
                    socket->Close();

                    socket.reset();
                }
            }

            void DataChannel::InternalSyncMessage(interop::InteropUnpooledMemory& mem, int32_t timeout)
            {
                common::concurrent::CsLockGuard lock(ioMutex);

                bool success = Send(mem.Data(), mem.Length(), timeout);

                if (!success)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Can not send message to remote host: timeout");

                success = Receive(mem, timeout);

                if (!success)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Can not send message to remote host: timeout");
            }

            bool DataChannel::Send(const int8_t* data, size_t len, int32_t timeout)
            {
                if (socket.get() == 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "Connection is not established");

                OperationResult::T res = SendAll(data, len, timeout);

                if (res == OperationResult::TIMEOUT)
                    return false;

                if (res == OperationResult::FAIL)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Can not send message due to connection failure");

                return true;
            }

            DataChannel::OperationResult::T DataChannel::SendAll(const int8_t* data, size_t len, int32_t timeout)
            {
                int sent = 0;

                while (sent != static_cast<int64_t>(len))
                {
                    int res = socket->Send(data + sent, len - sent, timeout);

                    if (res < 0 || res == SocketClient::WaitResult::TIMEOUT)
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
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not receive message header");

                interop::InteropInputStream inStream(&msg);

                int32_t msgLen = inStream.ReadInt32();

                if (msgLen < 0)
                {
                    Close();

                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Protocol error: Message length is negative");
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
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
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

                    if (res < 0 || res == SocketClient::WaitResult::TIMEOUT)
                    {
                        Close();

                        return res < 0 ? OperationResult::FAIL : OperationResult::TIMEOUT;
                    }

                    remain -= static_cast<size_t>(res);
                }

                return OperationResult::SUCCESS;
            }

            bool DataChannel::MakeRequestHandshake(const ProtocolVersion& propVer,
                ProtocolVersion& resVer, int32_t timeout)
            {
                currentVersion = propVer;

                resVer = ProtocolVersion();
                bool accepted = false;

                try
                {
                    // Workaround for some Linux systems that report connection on non-blocking
                    // sockets as successful but fail to establish real connection.
                    accepted = Handshake(propVer, resVer, timeout);
                }
                catch (const IgniteError&)
                {
                    return false;
                }

                if (!accepted)
                    return false;

                resVer = propVer;

                return true;
            }

            bool DataChannel::Handshake(const ProtocolVersion& propVer, ProtocolVersion& resVer, int32_t timeout)
            {
                // Allocating 4KB just in case.
                enum { BUFFER_SIZE = 1024 * 4 };

                common::concurrent::CsLockGuard lock(ioMutex);

                interop::InteropUnpooledMemory mem(BUFFER_SIZE);
                interop::InteropOutputStream outStream(&mem);
                binary::BinaryWriterImpl writer(&outStream, 0);

                int32_t lenPos = outStream.Reserve(4);
                writer.WriteInt8(RequestType::HANDSHAKE);

                writer.WriteInt16(propVer.GetMajor());
                writer.WriteInt16(propVer.GetMinor());
                writer.WriteInt16(propVer.GetMaintenance());

                writer.WriteInt8(ClientType::THIN_CLIENT);

                writer.WriteString(config.GetUser());
                writer.WriteString(config.GetPassword());

                outStream.WriteInt32(lenPos, outStream.Position() - 4);

                bool success = Send(mem.Data(), outStream.Position(), timeout);

                if (!success)
                    return false;

                success = Receive(mem, timeout);

                if (!success)
                    return false;

                interop::InteropInputStream inStream(&mem);

                inStream.Position(4);

                binary::BinaryReaderImpl reader(&inStream);

                bool accepted = reader.ReadBool();

                if (!accepted)
                {
                    int16_t major = reader.ReadInt16();
                    int16_t minor = reader.ReadInt16();
                    int16_t maintenance = reader.ReadInt16();

                    resVer = ProtocolVersion(major, minor, maintenance);

                    std::string error;
                    reader.ReadString(error);

                    reader.ReadInt32();

                    return false;
                }

                return true;
            }

            bool DataChannel::EnsureConnected(int32_t timeout)
            {
                if (socket.get() != 0)
                    return false;

                return TryRestoreConnection(timeout);
            }

            bool DataChannel::NegotiateProtocolVersion(int32_t timeout)
            {
                ProtocolVersion resVer;
                ProtocolVersion propVer = VERSION_CURRENT;

                bool success = MakeRequestHandshake(propVer, resVer, timeout);

                while (!success)
                {
                    if (resVer == propVer || !IsVersionSupported(resVer))
                        return false;

                    propVer = resVer;

                    success = MakeRequestHandshake(propVer, resVer, timeout);
                }

                return true;
            }

            bool DataChannel::TryRestoreConnection(int32_t timeout)
            {
                bool connected = false;

                connected = socket->Connect(address.host.c_str(), address.port, timeout);

                if (!connected)
                {
                    Close();

                    return false;
                }

                connected = NegotiateProtocolVersion(timeout);

                if (!connected)
                {
                    Close();

                    return false;
                }

                return true;
            }

            bool DataChannel::IsVersionSupported(const ProtocolVersion& ver)
            {
                return supportedVersions.find(ver) != supportedVersions.end();
            }
        }
    }
}

