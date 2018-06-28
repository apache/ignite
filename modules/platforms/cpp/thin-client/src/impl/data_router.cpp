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

namespace
{
#pragma pack(push, 1)
    struct OdbcProtocolHeader
    {
        int32_t len;
    };
#pragma pack(pop)
}


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
                socket(),
                timeout(0),
                loginTimeout(DEFALT_CONNECT_TIMEOUT),
                parser(VERSION_CURRENT),
                config(cfg)
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

                int32_t newLen = static_cast<int32_t>(len + sizeof(OdbcProtocolHeader));

                common::FixedSizeArray<int8_t> msg(newLen);

                OdbcProtocolHeader *hdr = reinterpret_cast<OdbcProtocolHeader*>(msg.GetData());

                hdr->len = static_cast<int32_t>(len);

                memcpy(msg.GetData() + sizeof(OdbcProtocolHeader), data, len);

                OperationResult::T res = SendAll(msg.GetData(), msg.GetSize(), timeout);

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

            bool DataRouter::Receive(std::vector<int8_t>& msg, int32_t timeout)
            {
                if (socket.get() == 0)
                    throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "DataRouter is not established");

                msg.clear();

                OdbcProtocolHeader hdr = { 0 };

                OperationResult::T res = ReceiveAll(reinterpret_cast<int8_t*>(&hdr), sizeof(hdr), timeout);

                if (res == OperationResult::TIMEOUT)
                    return false;

                if (res == OperationResult::FAIL)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not receive message header");

                if (hdr.len < 0)
                {
                    Close();

                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Protocol error: Message length is negative");
                }

                if (hdr.len == 0)
                    return false;

                msg.resize(hdr.len);

                res = ReceiveAll(&msg[0], hdr.len, timeout);

                if (res == OperationResult::TIMEOUT)
                    return false;

                if (res == OperationResult::FAIL)
                    throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not receive message body");

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
                HandshakeRequest req(config);
                HandshakeResponse rsp;

                parser.SetProtocolVersion(propVer);

                resVer = ProtocolVersion();

                try
                {
                    // Workaround for some Linux systems that report connection on non-blocking
                    // sockets as successful but fail to establish real connection.
                    bool sent = InternalSyncMessage(req, rsp, loginTimeout);

                    if (!sent)
                    {
                        //TODO: implement logging
                        //"Failed to get handshake response (Did you forget to enable SSL?).");

                        return false;
                    }
                }
                catch (const IgniteError&)
                {
                    //TODO: implement logging
                    return false;
                }

                if (!rsp.IsAccepted())
                {
                    //TODO: implement logging
                    //constructor << "Node rejected handshake message. ";

                    //if (!rsp.GetError().empty())
                    //    constructor << "Additional info: " << rsp.GetError() << " ";

                    //constructor << "Current node Apache Ignite version: " << rsp.GetCurrentVer().ToString() << ", "
                    //            << "driver protocol version introduced in version: " << protocolVersion.ToString() << ".";

                    resVer = rsp.GetCurrentVer();

                    return false;
                }

                resVer = propVer;

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
                        connected = socket->Connect(addr.host.c_str(), port, loginTimeout);

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

