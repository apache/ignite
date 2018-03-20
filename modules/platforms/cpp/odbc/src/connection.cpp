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

#include "ignite/odbc/log.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/statement.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/ssl/ssl_mode.h"
#include "ignite/odbc/ssl/ssl_gateway.h"
#include "ignite/odbc/ssl/secure_socket_client.h"
#include "ignite/odbc/system/tcp_socket_client.h"
#include "ignite/odbc/dsn_config.h"
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/connection_string_parser.h"

// Uncomment for per-byte debug.
//#define PER_BYTE_DEBUG

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
    namespace odbc
    {
        Connection::Connection() :
            socket(),
            timeout(0),
            parser(),
            config(),
            info(config)
        {
            // No-op.
        }

        Connection::~Connection()
        {
            // No-op.
        }

        const config::ConnectionInfo& Connection::GetInfo() const
        {
            return info;
        }

        void Connection::GetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen)
        {
            LOG_MSG("SQLGetInfo called: "
                << type << " ("
                << config::ConnectionInfo::InfoTypeToString(type) << "), "
                << std::hex << reinterpret_cast<size_t>(buf) << ", "
                << buflen << ", "
                << std::hex << reinterpret_cast<size_t>(reslen));

            IGNITE_ODBC_API_CALL(InternalGetInfo(type, buf, buflen, reslen));
        }

        SqlResult::Type Connection::InternalGetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen)
        {
            const config::ConnectionInfo& info = GetInfo();

            SqlResult::Type res = info.GetInfo(type, buf, buflen, reslen);

            if (res != SqlResult::AI_SUCCESS)
                AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Not implemented.");

            return res;
        }

        void Connection::Establish(const std::string& connectStr)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(connectStr));
        }

        SqlResult::Type Connection::InternalEstablish(const std::string& connectStr)
        {
            config::Configuration config;

            config::ConnectionStringParser parser(config);

            parser.ParseConnectionString(connectStr, &GetDiagnosticRecords());

            std::string dsn = config.GetDsn();

            if (!dsn.empty())
                ReadDsnConfiguration(dsn.c_str(), config);

            return InternalEstablish(config);
        }

        void Connection::Establish(const config::Configuration cfg)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(cfg));
        }

        SqlResult::Type Connection::InternalEstablish(const config::Configuration& cfg)
        {
            using ssl::SslMode;

            config = cfg;

            if (socket.get() != 0)
            {
                AddStatusRecord(SqlState::S08002_ALREADY_CONNECTED, "Already connected.");

                return SqlResult::AI_ERROR;
            }

            if (!config.IsHostSet() && config.IsAddressesSet() && config.GetAddresses().empty())
            {
                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, "No valid address to connect.");

                return SqlResult::AI_ERROR;
            }

            SslMode::Type sslMode = config.GetSslMode();

            if (sslMode != SslMode::DISABLE)
            {
                bool loaded = ssl::SslGateway::GetInstance().LoadAll();

                if (!loaded)
                {
                    AddStatusRecord(SqlState::SHY000_GENERAL_ERROR,
                        "Can not load OpenSSL library (did you set OPENSSL_HOME environment variable?).");

                    return SqlResult::AI_ERROR;
                }

                socket.reset(new ssl::SecureSocketClient(config.GetSslCertFile(),
                    config.GetSslKeyFile(), config.GetSslCaFile()));
            }
            else
                socket.reset(new system::TcpSocketClient());

            bool connected = TryRestoreConnection();

            if (!connected)
            {
                AddStatusRecord(SqlState::S08001_CANNOT_CONNECT, "Failed to establish connection with the host.");

                return SqlResult::AI_ERROR;
            }

            bool errors = GetDiagnosticRecords().GetStatusRecordsNumber() > 0;

            return errors ? SqlResult::AI_SUCCESS_WITH_INFO : SqlResult::AI_SUCCESS;
        }

        void Connection::Release()
        {
            IGNITE_ODBC_API_CALL(InternalRelease());
        }

        SqlResult::Type Connection::InternalRelease()
        {
            if (socket.get() == 0)
            {
                AddStatusRecord(SqlState::S08003_NOT_CONNECTED, "Connection is not open.");

                return SqlResult::AI_ERROR;
            }

            Close();

            return SqlResult::AI_SUCCESS;
        }

        void Connection::Close()
        {
            if (socket.get() != 0)
            {
                socket->Close();

                socket.reset();
            }
        }

        Statement* Connection::CreateStatement()
        {
            Statement* statement;

            IGNITE_ODBC_API_CALL(InternalCreateStatement(statement));

            return statement;
        }

        SqlResult::Type Connection::InternalCreateStatement(Statement*& statement)
        {
            statement = new Statement(*this);

            if (!statement)
            {
                AddStatusRecord(SqlState::SHY001_MEMORY_ALLOCATION, "Not enough memory.");

                return SqlResult::AI_ERROR;
            }

            return SqlResult::AI_SUCCESS;
        }

        bool Connection::Send(const int8_t* data, size_t len, int32_t timeout)
        {
            if (socket.get() == 0)
                throw OdbcError(SqlState::S08003_NOT_CONNECTED, "Connection is not established");

            int32_t newLen = static_cast<int32_t>(len + sizeof(OdbcProtocolHeader));

            common::FixedSizeArray<int8_t> msg(newLen);

            OdbcProtocolHeader *hdr = reinterpret_cast<OdbcProtocolHeader*>(msg.GetData());

            hdr->len = static_cast<int32_t>(len);

            memcpy(msg.GetData() + sizeof(OdbcProtocolHeader), data, len);

            OperationResult::T res = SendAll(msg.GetData(), msg.GetSize(), timeout);

            if (res == OperationResult::TIMEOUT)
                return false;

            if (res == OperationResult::FAIL)
                throw OdbcError(SqlState::S08S01_LINK_FAILURE, "Can not send message due to connection failure");

#ifdef PER_BYTE_DEBUG
            LOG_MSG("message sent: (" <<  msg.GetSize() << " bytes)" << utility::HexDump(msg.GetData(), msg.GetSize()));
#endif //PER_BYTE_DEBUG

            return true;
        }

        Connection::OperationResult::T Connection::SendAll(const int8_t* data, size_t len, int32_t timeout)
        {
            int sent = 0;

            while (sent != static_cast<int64_t>(len))
            {
                int res = socket->Send(data + sent, len - sent, timeout);

                LOG_MSG("Sent: " << res);

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

        bool Connection::Receive(std::vector<int8_t>& msg, int32_t timeout)
        {
            if (socket.get() == 0)
                throw OdbcError(SqlState::S08003_NOT_CONNECTED, "Connection is not established");

            msg.clear();

            OdbcProtocolHeader hdr;

            OperationResult::T res = ReceiveAll(reinterpret_cast<int8_t*>(&hdr), sizeof(hdr), timeout);

            if (res == OperationResult::TIMEOUT)
                return false;

            if (res == OperationResult::FAIL)
                throw OdbcError(SqlState::S08S01_LINK_FAILURE, "Can not receive message header");

            if (hdr.len < 0)
            {
                Close();

                throw OdbcError(SqlState::SHY000_GENERAL_ERROR, "Protocol error: Message length is negative");
            }

            if (hdr.len == 0)
                return false;

            msg.resize(hdr.len);

            res = ReceiveAll(&msg[0], hdr.len, timeout);

            if (res == OperationResult::TIMEOUT)
                return false;

            if (res == OperationResult::FAIL)
                throw OdbcError(SqlState::S08S01_LINK_FAILURE, "Can not receive message body");

#ifdef PER_BYTE_DEBUG
            LOG_MSG("Message received: " << utility::HexDump(&msg[0], msg.size()));
#endif //PER_BYTE_DEBUG

            return true;
        }

        Connection::OperationResult::T Connection::ReceiveAll(void* dst, size_t len, int32_t timeout)
        {
            size_t remain = len;
            int8_t* buffer = reinterpret_cast<int8_t*>(dst);

            while (remain)
            {
                size_t received = len - remain;

                int res = socket->Receive(buffer + received, remain, timeout);
                LOG_MSG("Receive res: " << res << " remain: " << remain);

                if (res < 0 || res == SocketClient::WaitResult::TIMEOUT)
                {
                    Close();

                    return res < 0 ? OperationResult::FAIL : OperationResult::TIMEOUT;
                }

                remain -= static_cast<size_t>(res);
            }

            return OperationResult::SUCCESS;
        }

        const std::string& Connection::GetSchema() const
        {
            return config.GetSchema();
        }

        const config::Configuration& Connection::GetConfiguration() const
        {
            return config;
        }

        diagnostic::DiagnosticRecord Connection::CreateStatusRecord(SqlState::Type sqlState,
            const std::string& message, int32_t rowNum, int32_t columnNum)
        {
            return diagnostic::DiagnosticRecord(sqlState, message, "", "", rowNum, columnNum);
        }

        void Connection::TransactionCommit()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionCommit());
        }

        SqlResult::Type Connection::InternalTransactionCommit()
        {
            return SqlResult::AI_SUCCESS;
        }

        void Connection::TransactionRollback()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionRollback());
        }

        SqlResult::Type Connection::InternalTransactionRollback()
        {
            AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Rollback operation is not supported.");

            return SqlResult::AI_ERROR;
        }

        void Connection::GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buf, bufLen, valueLen));
        }

        SqlResult::Type Connection::InternalGetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            if (!buf)
            {
                AddStatusRecord(SqlState::SHY009_INVALID_USE_OF_NULL_POINTER, "Data buffer is null.");

                return SqlResult::AI_ERROR;
            }

            switch (attr)
            {
                case SQL_ATTR_CONNECTION_DEAD:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = socket.get() != 0 ? SQL_CD_FALSE : SQL_CD_TRUE;

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_CONNECTION_TIMEOUT:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = static_cast<SQLUINTEGER>(timeout);

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                default:
                {
                    AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return SqlResult::AI_ERROR;
                }
            }

            return SqlResult::AI_SUCCESS;
        }

        void Connection::SetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalSetAttribute(attr, value, valueLen));
        }

        SqlResult::Type Connection::InternalSetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            if (!value)
            {
                AddStatusRecord(SqlState::SHY009_INVALID_USE_OF_NULL_POINTER, "Value pointer is null.");

                return SqlResult::AI_ERROR;
            }

            switch (attr)
            {
                case SQL_ATTR_CONNECTION_DEAD:
                {
                    AddStatusRecord(SqlState::SHY092_OPTION_TYPE_OUT_OF_RANGE, "Attribute is read only.");

                    return SqlResult::AI_ERROR;
                }

                case SQL_ATTR_CONNECTION_TIMEOUT:
                {
                    SQLUINTEGER uTimeout = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));

                    if (uTimeout != 0 && socket.get() != 0 && socket->IsBlocking())
                    {
                        timeout = 0;

                        AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, "Can not set timeout, because can not "
                            "enable non-blocking mode on TCP connection. Setting to 0.");

                        return SqlResult::AI_SUCCESS_WITH_INFO;
                    }

                    if (uTimeout > INT32_MAX)
                    {
                        timeout = INT32_MAX;

                        std::stringstream ss;

                        ss << "Value is too big: " << uTimeout << ", changing to " << timeout << ".";
                        std::string msg = ss.str();

                        AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, msg);

                        return SqlResult::AI_SUCCESS_WITH_INFO;
                    }

                    timeout = static_cast<int32_t>(uTimeout);

                    break;
                }

                default:
                {
                    AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                        "Specified attribute is not supported.");

                    return SqlResult::AI_ERROR;
                }
            }

            return SqlResult::AI_SUCCESS;
        }

        SqlResult::Type Connection::MakeRequestHandshake()
        {
            ProtocolVersion protocolVersion = config.GetProtocolVersion();

            if (!protocolVersion.IsSupported())
            {
                AddStatusRecord(SqlState::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                    "Protocol version is not supported: " + protocolVersion.ToString());

                return SqlResult::AI_ERROR;
            }

            bool distributedJoins = config.IsDistributedJoins();
            bool enforceJoinOrder = config.IsEnforceJoinOrder();
            bool replicatedOnly = config.IsReplicatedOnly();
            bool collocated = config.IsCollocated();
            bool lazy = config.IsLazy();
            bool skipReducerOnUpdate = config.IsSkipReducerOnUpdate();

            HandshakeRequest req(protocolVersion, distributedJoins, enforceJoinOrder, replicatedOnly, collocated, lazy,
                skipReducerOnUpdate);
            HandshakeResponse rsp;

            try
            {
                // Workaround for some Linux systems that report connection on non-blocking
                // sockets as successful but fail to establish real connection.
                bool sent = InternalSyncMessage(req, rsp, SocketClient::CONNECT_TIMEOUT);

                if (!sent)
                {
                    AddStatusRecord(SqlState::S08001_CANNOT_CONNECT,
                        "Failed to get handshake response (Did you forget to enable SSL?).");

                    return SqlResult::AI_ERROR;
                }
            }
            catch (const OdbcError& err)
            {
                AddStatusRecord(err);

                return SqlResult::AI_ERROR;
            }
            catch (const IgniteError& err)
            {
                AddStatusRecord(SqlState::S08004_CONNECTION_REJECTED, err.GetText());

                return SqlResult::AI_ERROR;
            }

            if (!rsp.IsAccepted())
            {
                LOG_MSG("Handshake message has been rejected.");

                std::stringstream constructor;

                constructor << "Node rejected handshake message. ";

                if (!rsp.GetError().empty())
                    constructor << "Additional info: " << rsp.GetError() << " ";

                constructor << "Current node Apache Ignite version: " << rsp.GetCurrentVer().ToString() << ", "
                            << "driver protocol version introduced in version: "
                            << config.GetProtocolVersion().ToString() << ".";

                AddStatusRecord(SqlState::S08004_CONNECTION_REJECTED, constructor.str());

                return SqlResult::AI_ERROR;
            }

            return SqlResult::AI_SUCCESS;
        }

        void Connection::EnsureConnected()
        {
            if (socket.get() != 0)
                return;

            bool success = TryRestoreConnection();

            if (!success)
                throw OdbcError(SqlState::S08001_CANNOT_CONNECT,
                    "Failed to establish connection with any provided hosts");
        }

        bool Connection::TryRestoreConnection()
        {
            std::vector<EndPoint> addrs;

            CollectAddresses(config, addrs);

            bool connected = false;

            while (!addrs.empty() && !connected)
            {
                const EndPoint& addr = addrs.back();

                for (uint16_t port = addr.port; port <= addr.port + addr.range; ++port)
                {
                    connected = socket->Connect(addr.host.c_str(), port, *this);

                    if (connected)
                    {
                        SqlResult::Type res = MakeRequestHandshake();

                        connected = res != SqlResult::AI_ERROR;

                        if (connected)
                            break;
                    }
                }

                addrs.pop_back();
            }

            if (!connected)
                Close();
            else
                parser.SetProtocolVersion(config.GetProtocolVersion());

            return connected;
        }

        void Connection::CollectAddresses(const config::Configuration& cfg, std::vector<EndPoint>& endPoints)
        {
            endPoints.clear();

            if (!cfg.IsAddressesSet())
            {
                LOG_MSG("'Address' is not set. Using legacy connection method.");

                endPoints.push_back(EndPoint(cfg.GetHost(), cfg.GetTcpPort()));

                return;
            }

            endPoints = cfg.GetAddresses();

            std::random_shuffle(endPoints.begin(), endPoints.end());
        }
    }
}

