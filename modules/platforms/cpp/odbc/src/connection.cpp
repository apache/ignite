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

#include <sstream>

#include <ignite/common/fixed_size_array.h>

#include "ignite/odbc/log.h"
#include "ignite/odbc/utility.h"
#include "ignite/odbc/statement.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/message.h"
#include "ignite/odbc/config/configuration.h"

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
            connected(false),
            parser(),
            config()
        {
            // No-op.
        }

        Connection::~Connection()
        {
            // No-op.
        }

        const config::ConnectionInfo& Connection::GetInfo() const
        {
            // Connection info is constant and the same for all connections now.
            const static config::ConnectionInfo info;

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

            try
            {
                config.FillFromConnectString(connectStr);
            }
            catch (IgniteError& e)
            {
                AddStatusRecord(SqlState::SHY000_GENERAL_ERROR, e.GetText());

                return SqlResult::AI_ERROR;
            }

            return InternalEstablish(config);
        }

        void Connection::Establish(const config::Configuration cfg)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(cfg));
        }

        SqlResult::Type Connection::InternalEstablish(const config::Configuration cfg)
        {
            config = cfg;

            if (connected)
            {
                AddStatusRecord(SqlState::S08002_ALREADY_CONNECTED, "Already connected.");

                return SqlResult::AI_ERROR;
            }

            connected = socket.Connect(cfg.GetHost().c_str(), cfg.GetTcpPort());

            if (!connected)
            {
                AddStatusRecord(SqlState::S08001_CANNOT_CONNECT, "Failed to establish connection with the host.");

                return SqlResult::AI_ERROR;
            }

            return MakeRequestHandshake();
        }

        void Connection::Release()
        {
            IGNITE_ODBC_API_CALL(InternalRelease());
        }

        SqlResult::Type Connection::InternalRelease()
        {
            if (!connected)
            {
                AddStatusRecord(SqlState::S08003_NOT_CONNECTED, "Connection is not open.");

                return SqlResult::AI_ERROR;
            }

            socket.Close();

            connected = false;

            return SqlResult::AI_SUCCESS;
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

        void Connection::Send(const int8_t* data, size_t len)
        {
            if (!connected)
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "Connection is not established");

            int32_t newLen = static_cast<int32_t>(len + sizeof(OdbcProtocolHeader));

            common::FixedSizeArray<int8_t> msg(newLen);

            OdbcProtocolHeader *hdr = reinterpret_cast<OdbcProtocolHeader*>(msg.GetData());

            hdr->len = static_cast<int32_t>(len);

            memcpy(msg.GetData() + sizeof(OdbcProtocolHeader), data, len);

            size_t sent = SendAll(msg.GetData(), msg.GetSize());

            if (sent != len + sizeof(OdbcProtocolHeader))
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_GENERIC, "Can not send message");

            LOG_MSG("message sent: (" <<  msg.GetSize() << " bytes)" << utility::HexDump(msg.GetData(), msg.GetSize()));
        }

        size_t Connection::SendAll(const int8_t* data, size_t len)
        {
            int sent = 0;

            while (sent != static_cast<int64_t>(len))
            {
                int res = socket.Send(data + sent, len - sent);

                LOG_MSG("Sent: " << res);

                if (res <= 0)
                    return sent;

                sent += res;
            }

            return sent;
        }

        void Connection::Receive(std::vector<int8_t>& msg)
        {
            if (!connected)
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_ILLEGAL_STATE, "Connection is not established");

            msg.clear();

            OdbcProtocolHeader hdr;

            size_t received = ReceiveAll(reinterpret_cast<int8_t*>(&hdr), sizeof(hdr));

            if (received != sizeof(hdr))
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_GENERIC, "Can not receive message header");

            if (hdr.len < 0)
                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_GENERIC, "Message lenght is negative");

            if (hdr.len == 0)
                return;

            msg.resize(hdr.len);

            received = ReceiveAll(&msg[0], hdr.len);

            if (received != hdr.len)
            {
                msg.resize(received);

                IGNITE_ERROR_1(IgniteError::IGNITE_ERR_GENERIC, "Can not receive message body");
            }

            LOG_MSG("Message received: " << utility::HexDump(&msg[0], msg.size()));
        }

        size_t Connection::ReceiveAll(void* dst, size_t len)
        {
            size_t remain = len;
            int8_t* buffer = reinterpret_cast<int8_t*>(dst);

            while (remain)
            {
                size_t received = len - remain;

                int res = socket.Receive(buffer + received, remain);
                LOG_MSG("Receive res: " << res << " remain: " << remain);

                if (res <= 0)
                    return received;

                remain -= static_cast<size_t>(res);
            }

            return len;
        }

        const std::string& Connection::GetCache() const
        {
            return config.GetCache();
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

        SqlResult::Type Connection::MakeRequestHandshake()
        {
            bool distributedJoins = false;
            bool enforceJoinOrder = false;
            ProtocolVersion protocolVersion;

            try
            {
                distributedJoins = config.IsDistributedJoins();
                enforceJoinOrder = config.IsEnforceJoinOrder();
                protocolVersion = config.GetProtocolVersion();
            }
            catch (const IgniteError& err)
            {
                AddStatusRecord(SqlState::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE, err.GetText());

                return SqlResult::AI_ERROR;
            }

            if (!protocolVersion.IsSupported())
            {
                AddStatusRecord(SqlState::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE, "Protocol version is not supported: " + protocolVersion.ToString());

                return SqlResult::AI_ERROR;
            }

            HandshakeRequest req(protocolVersion, distributedJoins, enforceJoinOrder);
            HandshakeResponse rsp;

            try
            {
                SyncMessage(req, rsp);
            }
            catch (const IgniteError& err)
            {
                AddStatusRecord(SqlState::SHYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                return SqlResult::AI_ERROR;
            }

            if (!rsp.IsAccepted())
            {
                LOG_MSG("Hanshake message has been rejected.");

                std::stringstream constructor;

                constructor << "Node rejected handshake message. ";

                if (!rsp.GetError().empty())
                    constructor << "Additional info: " << rsp.GetError();

                constructor << "Current node Apache Ignite version: " << rsp.GetCurrentVer().ToString() << ", "
                            << "driver protocol version introduced in version: " << config.GetProtocolVersion().ToString() << ".";

                AddStatusRecord(SqlState::S08001_CANNOT_CONNECT, constructor.str());

                InternalRelease();

                return SqlResult::AI_ERROR;
            }

            return SqlResult::AI_SUCCESS;
        }
    }
}

