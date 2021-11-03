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

#include <ignite/network/network.h>

#include "ignite/odbc/log.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/environment.h"
#include "ignite/odbc/statement.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/dsn_config.h"
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/system/system_dsn.h"

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
        Connection::Connection(Environment* env) :
            env(env),
            socket(),
            timeout(0),
            loginTimeout(DEFAULT_CONNECT_TIMEOUT),
            autoCommit(true),
            parser(),
            config(),
            info(config),
            streamingContext()
        {
            streamingContext.SetConnection(*this);
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
                << std::hex << reinterpret_cast<size_t>(reslen)
                << std::dec);

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

        void Connection::Establish(const std::string& connectStr, void* parentWindow)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(connectStr, parentWindow));
        }

        SqlResult::Type Connection::InternalEstablish(const std::string& connectStr, void* parentWindow)
        {
            config::Configuration config;
            config::ConnectionStringParser parser(config);
            parser.ParseConnectionString(connectStr, &GetDiagnosticRecords());

            if (parentWindow)
            {
                LOG_MSG("Parent window is passed. Creating configuration window.");
                if (!DisplayConnectionWindow(parentWindow, config))
                {
                    AddStatusRecord(odbc::SqlState::SHY008_OPERATION_CANCELED, "Connection canceled by user");

                    return SqlResult::AI_ERROR;
                }
            }

            if (config.IsDsnSet())
            {
                std::string dsn = config.GetDsn();

                ReadDsnConfiguration(dsn.c_str(), config, &GetDiagnosticRecords());
            }

            return InternalEstablish(config);
        }

        void Connection::Establish(const config::Configuration cfg)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(cfg));
        }

        SqlResult::Type Connection::InitSocket()
        {
            ssl::SslMode::Type sslMode = config.GetSslMode();

            if (sslMode == ssl::SslMode::DISABLE)
            {
                socket.reset(network::ssl::MakeTcpSocketClient());

                return SqlResult::AI_SUCCESS;
            }

            try
            {
                network::ssl::EnsureSslLoaded();
            }
            catch (const IgniteError &err)
            {
                LOG_MSG("Can not load OpenSSL library: " << err.GetText());

                AddStatusRecord("Can not load OpenSSL library (did you set OPENSSL_HOME environment variable?)");

                return SqlResult::AI_ERROR;
            }

            socket.reset(network::ssl::MakeSecureSocketClient(
                config.GetSslCertFile(), config.GetSslKeyFile(), config.GetSslCaFile()));

            return SqlResult::AI_SUCCESS;
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
                AddStatusRecord("No valid address to connect.");

                return SqlResult::AI_ERROR;
            }

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

        void Connection::Deregister()
        {
            env->DeregisterConnection(this);
        }

        SqlResult::Type Connection::InternalRelease()
        {
            if (socket.get() == 0)
            {
                AddStatusRecord(SqlState::S08003_NOT_CONNECTED, "Connection is not open.");

                // It is important to return SUCCESS_WITH_INFO and not ERROR here, as if we return an error, Windows
                // Driver Manager may decide that connection is not valid anymore which results in memory leak.
                return SqlResult::AI_SUCCESS_WITH_INFO;
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

                if (res < 0 || res == network::SocketClient::WaitResult::TIMEOUT)
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

        bool Connection::IsAutoCommit() const
        {
            return autoCommit;
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
            std::string schema = config.GetSchema();

            app::ParameterSet empty;

            QueryExecuteRequest req(schema, "COMMIT", empty, timeout, autoCommit);
            QueryExecuteResponse rsp;

            try
            {
                bool sent = SyncMessage(req, rsp, timeout);

                if (!sent)
                {
                    AddStatusRecord(SqlState::S08S01_LINK_FAILURE, "Failed to send commit request.");

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
                AddStatusRecord(err.GetText());

                return SqlResult::AI_ERROR;
            }

            return SqlResult::AI_SUCCESS;
        }

        void Connection::TransactionRollback()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionRollback());
        }

        SqlResult::Type Connection::InternalTransactionRollback()
        {
            std::string schema = config.GetSchema();

            app::ParameterSet empty;

            QueryExecuteRequest req(schema, "ROLLBACK", empty, timeout, autoCommit);
            QueryExecuteResponse rsp;

            try
            {
                bool sent = SyncMessage(req, rsp, timeout);

                if (!sent)
                {
                    AddStatusRecord(SqlState::S08S01_LINK_FAILURE, "Failed to send rollback request.");

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
                AddStatusRecord(err.GetText());

                return SqlResult::AI_ERROR;
            }

            return SqlResult::AI_SUCCESS;
        }

        void Connection::GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buf, bufLen, valueLen));
        }

        SqlResult::Type Connection::InternalGetAttribute(int attr, void* buf, SQLINTEGER, SQLINTEGER* valueLen)
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

                case SQL_ATTR_LOGIN_TIMEOUT:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = static_cast<SQLUINTEGER>(loginTimeout);

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_AUTOCOMMIT:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = autoCommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;

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

        SqlResult::Type Connection::InternalSetAttribute(int attr, void* value, SQLINTEGER)
        {
            switch (attr)
            {
                case SQL_ATTR_CONNECTION_DEAD:
                {
                    AddStatusRecord(SqlState::SHY092_OPTION_TYPE_OUT_OF_RANGE, "Attribute is read only.");

                    return SqlResult::AI_ERROR;
                }

                case SQL_ATTR_CONNECTION_TIMEOUT:
                {
                    timeout = RetrieveTimeout(value);

                    if (GetDiagnosticRecords().GetStatusRecordsNumber() != 0)
                        return SqlResult::AI_SUCCESS_WITH_INFO;

                    break;
                }

                case SQL_ATTR_LOGIN_TIMEOUT:
                {
                    loginTimeout = RetrieveTimeout(value);

                    if (GetDiagnosticRecords().GetStatusRecordsNumber() != 0)
                        return SqlResult::AI_SUCCESS_WITH_INFO;

                    break;
                }

                case SQL_ATTR_AUTOCOMMIT:
                {
                    SQLUINTEGER mode = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));

                    if (mode != SQL_AUTOCOMMIT_ON && mode != SQL_AUTOCOMMIT_OFF)
                    {
                        AddStatusRecord(SqlState::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Specified attribute is not supported.");

                        return SqlResult::AI_ERROR;
                    }

                    autoCommit = mode == SQL_AUTOCOMMIT_ON;

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

            if (protocolVersion < ProtocolVersion::VERSION_2_5_0 && !config.GetUser().empty())
            {
                AddStatusRecord(SqlState::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                    "Authentication is not allowed for protocol version below 2.5.0");

                return SqlResult::AI_ERROR;
            }

            HandshakeRequest req(config);
            HandshakeResponse rsp;

            try
            {
                // Workaround for some Linux systems that report connection on non-blocking
                // sockets as successful but fail to establish real connection.
                bool sent = InternalSyncMessage(req, rsp, loginTimeout);

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

                constructor << "Current version of the protocol, used by the server node is "
                            << rsp.GetCurrentVer().ToString() << ", "
                            << "driver protocol version introduced in version "
                            << protocolVersion.ToString() << ".";

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

            if (socket.get() == 0)
            {
                SqlResult::Type res = InitSocket();

                if (res != SqlResult::AI_SUCCESS)
                    return false;
            }

            bool connected = false;

            while (!addrs.empty() && !connected)
            {
                const EndPoint& addr = addrs.back();

                for (uint16_t port = addr.port; port <= addr.port + addr.range; ++port)
                {
                    try
                    {
                        connected = socket->Connect(addr.host.c_str(), port, loginTimeout);
                    }
                    catch (const IgniteError& err)
                    {
                        LOG_MSG("Error while trying connect to " << addr.host << ":" << addr.port <<", " << err.GetText());
                    }

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

        int32_t Connection::RetrieveTimeout(void* value)
        {
            SQLUINTEGER uTimeout = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));

            if (uTimeout != 0 && socket.get() != 0 && socket->IsBlocking())
            {
                AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, "Can not set timeout, because can not "
                    "enable non-blocking mode on TCP connection. Setting to 0.");

                return 0;
            }

            if (uTimeout > INT32_MAX)
            {
                std::stringstream ss;

                ss << "Value is too big: " << uTimeout << ", changing to " << timeout << ".";

                AddStatusRecord(SqlState::S01S02_OPTION_VALUE_CHANGED, ss.str());

                return INT32_MAX;
            }

            return static_cast<int32_t>(uTimeout);
        }
    }
}

