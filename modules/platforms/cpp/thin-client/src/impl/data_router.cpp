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
#include <algorithm>

#include <ignite/common/fixed_size_array.h>

#include <ignite/impl/thin/utility.h>
#include <ignite/impl/thin/data_router.h>
#include <ignite/impl/thin/message.h>
#include <ignite/impl/thin/ssl/ssl_gateway.h>
#include <ignite/impl/thin/ssl/secure_socket_client.h>
#include <ignite/impl/thin/net/tcp_socket_client.h>


namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** Version 1.1.0. */
            const ProtocolVersion DataRouter::VERSION_1_1_0(1, 1, 0);

            /** Current version. */
            const ProtocolVersion DataRouter::VERSION_CURRENT(VERSION_1_1_0);

            DataRouter::VersionSet::value_type supportedArray[] = {
                DataRouter::VERSION_1_1_0,
            };

            const DataRouter::VersionSet DataRouter::supportedVersions(supportedArray,
                supportedArray + (sizeof(supportedArray) / sizeof(supportedArray[0])));

            DataRouter::DataRouter(const ignite::thin::IgniteClientConfiguration& cfg) :
                ioTimeout(DEFALT_IO_TIMEOUT),
                connectionTimeout(DEFALT_CONNECT_TIMEOUT),
                socket(),
                config(cfg),
                currentVersion(VERSION_CURRENT),
                reqIdCounter(0)
            {
                // No-op.
            }

            DataRouter::~DataRouter()
            {
                // No-op.
            }

            void DataRouter::Connect()
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

                bool connected = TryRestoreConnection();

                if (!connected)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Failed to establish connection with the host.");
            }

            void DataRouter::Close()
            {
                if (socket.get() != 0)
                {
                    socket->Close();

                    socket.reset();
                }
            }

            bool DataRouter::Send(const int8_t* data, size_t len, int32_t timeout)
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

            DataRouter::OperationResult::T DataRouter::SendAll(const int8_t* data, size_t len, int32_t timeout)
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

            bool DataRouter::Receive(interop::InteropMemory& msg, int32_t timeout)
            {
                assert(msg.Capacity() > 4);

                if (socket.get() == 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "DataRouter is not established");

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

            DataRouter::OperationResult::T DataRouter::ReceiveAll(void* dst, size_t len, int32_t timeout)
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

            bool DataRouter::MakeRequestHandshake(const ProtocolVersion& propVer, ProtocolVersion& resVer)
            {
                currentVersion = propVer;

                resVer = ProtocolVersion();
                bool accepted = false;

                try
                {
                    // Workaround for some Linux systems that report connection on non-blocking
                    // sockets as successful but fail to establish real connection.
                    accepted = Handshake(propVer, resVer);
                }
                catch (const IgniteError&)
                {
                    // TODO: implement logging
                    //std::cout << err.GetText() << std::endl;
                    return false;
                }

                if (!accepted)
                    return false;

                resVer = propVer;

                return true;
            }

            bool DataRouter::Handshake(const ProtocolVersion& propVer, ProtocolVersion& resVer)
            {
                // Allocating 4KB to lessen number of reallocations.
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

                bool success = Send(mem.Data(), outStream.Position(), connectionTimeout);

                if (!success)
                    return false;

                success = Receive(mem, connectionTimeout);

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

            void DataRouter::EnsureConnected()
            {
                if (socket.get() != 0)
                    return;

                bool success = TryRestoreConnection();

                if (!success)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                        "Failed to establish connection with any provided hosts");
            }

            bool DataRouter::NegotiateProtocolVersion()
            {
                ProtocolVersion resVer;
                ProtocolVersion propVer = VERSION_CURRENT;

                bool success = MakeRequestHandshake(propVer, resVer);

                while (!success)
                {
                    if (resVer == propVer || !IsVersionSupported(resVer))
                        return false;

                    propVer = resVer;

                    success = MakeRequestHandshake(propVer, resVer);
                }

                return true;
            }

            bool DataRouter::TryRestoreConnection()
            {
                std::vector<net::EndPoint> addrs;

                CollectAddresses(config.GetEndPoints(), addrs);

                bool connected = false;

                while (!addrs.empty() && !connected)
                {
                    const net::EndPoint& addr = addrs.back();

                    for (uint16_t port = addr.port; port <= addr.port + addr.range; ++port)
                    {
                        connected = socket->Connect(addr.host.c_str(), port, connectionTimeout);

                        if (connected)
                        {
                            connected = NegotiateProtocolVersion();

                            if (connected)
                                break;
                        }
                    }

                    addrs.pop_back();
                }

                if (!connected)
                    Close();

                return connected;
            }

            void DataRouter::CollectAddresses(const std::string& str, std::vector<net::EndPoint>& endPoints)
            {
                endPoints.clear();

                utility::ParseAddress(str, endPoints, DEFAULT_PORT);

                std::random_shuffle(endPoints.begin(), endPoints.end());
            }

            bool DataRouter::IsVersionSupported(const ProtocolVersion& ver)
            {
                return supportedVersions.find(ver) != supportedVersions.end();
            }
        }
    }
}

