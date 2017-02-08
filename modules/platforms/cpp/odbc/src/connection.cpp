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

        SqlResult Connection::InternalGetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen)
        {
            const config::ConnectionInfo& info = GetInfo();

            SqlResult res = info.GetInfo(type, buf, buflen, reslen);

            if (res != SQL_RESULT_SUCCESS)
                AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Not implemented.");

            return res;
        }

        void Connection::Establish(const std::string& connectStr)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(connectStr));
        }

        SqlResult Connection::InternalEstablish(const std::string& connectStr)
        {
            config::Configuration config;

            try
            {
                config.FillFromConnectString(connectStr);
            }
            catch (IgniteError& e)
            {
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, e.GetText());

                return SQL_RESULT_ERROR;
            }

            return InternalEstablish(config);
        }

        void Connection::Establish(const config::Configuration cfg)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(cfg));
        }

        SqlResult Connection::InternalEstablish(const config::Configuration cfg)
        {
            config = cfg;

            if (connected)
            {
                AddStatusRecord(SQL_STATE_08002_ALREADY_CONNECTED, "Already connected.");

                return SQL_RESULT_ERROR;
            }

            connected = socket.Connect(cfg.GetHost().c_str(), cfg.GetTcpPort());

            if (!connected)
            {
                AddStatusRecord(SQL_STATE_08001_CANNOT_CONNECT, "Failed to establish connection with the host.");

                return SQL_RESULT_ERROR;
            }

            return MakeRequestHandshake();
        }

        void Connection::Release()
        {
            IGNITE_ODBC_API_CALL(InternalRelease());
        }

        SqlResult Connection::InternalRelease()
        {
            if (!connected)
            {
                AddStatusRecord(SQL_STATE_08003_NOT_CONNECTED, "Connection is not open.");

                return SQL_RESULT_ERROR;
            }

            socket.Close();

            connected = false;

            return SQL_RESULT_SUCCESS;
        }

        Statement* Connection::CreateStatement()
        {
            Statement* statement;

            IGNITE_ODBC_API_CALL(InternalCreateStatement(statement));

            return statement;
        }

        SqlResult Connection::InternalCreateStatement(Statement*& statement)
        {
            statement = new Statement(*this);

            if (!statement)
            {
                AddStatusRecord(SQL_STATE_HY001_MEMORY_ALLOCATION, "Not enough memory.");

                return SQL_RESULT_ERROR;
            }

            return SQL_RESULT_SUCCESS;
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

        diagnostic::DiagnosticRecord Connection::CreateStatusRecord(SqlState sqlState,
            const std::string& message, int32_t rowNum, int32_t columnNum)
        {
            return diagnostic::DiagnosticRecord(sqlState, message, "", "", rowNum, columnNum);
        }

        void Connection::TransactionCommit()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionCommit());
        }

        SqlResult Connection::InternalTransactionCommit()
        {
            return SQL_RESULT_SUCCESS;
        }

        void Connection::TransactionRollback()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionRollback());
        }

        SqlResult Connection::InternalTransactionRollback()
        {
            AddStatusRecord(SQL_STATE_HYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Rollback operation is not supported.");

            return SQL_RESULT_ERROR;
        }

        SqlResult Connection::MakeRequestHandshake()
        {
            bool distributedJoins = false;
            bool enforceJoinOrder = false;
            int64_t protocolVersion = 0;

            try
            {
                distributedJoins = config.IsDistributedJoins();
                enforceJoinOrder = config.IsEnforceJoinOrder();
                protocolVersion = config.GetProtocolVersion().GetIntValue();
            }
            catch (const IgniteError& err)
            {
                AddStatusRecord(SQL_STATE_01S00_INVALID_CONNECTION_STRING_ATTRIBUTE, err.GetText());

                return SQL_RESULT_ERROR;
            }

            HandshakeRequest req(protocolVersion, distributedJoins, enforceJoinOrder);
            HandshakeResponse rsp;

            try
            {
                SyncMessage(req, rsp);
            }
            catch (const IgniteError& err)
            {
                AddStatusRecord(SQL_STATE_HYT01_CONNECTIOIN_TIMEOUT, err.GetText());

                return SQL_RESULT_ERROR;
            }

            if (rsp.GetStatus() != RESPONSE_STATUS_SUCCESS)
            {
                LOG_MSG("Error: " << rsp.GetError().c_str());

                AddStatusRecord(SQL_STATE_08001_CANNOT_CONNECT, rsp.GetError());

                InternalRelease();

                return SQL_RESULT_ERROR;
            }

            if (!rsp.IsAccepted())
            {
                LOG_MSG("Hanshake message has been rejected.");

                std::stringstream constructor;

                constructor << "Node rejected handshake message. "
                    << "Current node Apache Ignite version: " << rsp.CurrentVer() << ", "
                    << "node protocol version introduced in version: " << rsp.ProtoVerSince() << ", "
                    << "driver protocol version introduced in version: " << config.GetProtocolVersion().ToString() << ".";

                AddStatusRecord(SQL_STATE_08001_CANNOT_CONNECT, constructor.str());

                InternalRelease();

                return SQL_RESULT_ERROR;
            }

            return SQL_RESULT_SUCCESS;
        }
    }
}

