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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/statement.h"
#include "ignite/odbc/connection.h"
#include "ignite/odbc/config/configuration.h"

// TODO: implement appropriate protocol with de-/serialisation.
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
        Connection::Connection() : socket(), connected(false), cache(), parser()
        {
            // No-op.
        }

        Connection::~Connection()
        {
            // No-op.
        }
        
        const config::ConnectionInfo& Connection::GetInfo() const
        {
            // Connection info is the same for all connections now.
            static config::ConnectionInfo info;

            return info;
        }

        void Connection::GetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen)
        {
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

        void Connection::Establish(const std::string& server)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(server));
        }

        SqlResult Connection::InternalEstablish(const std::string& server)
        {
            config::Configuration config;

            if (server != config.GetDsn())
            {
                AddStatusRecord(SQL_STATE_HY000_GENERAL_ERROR, "Unknown DNS.");

                return SQL_RESULT_ERROR;
            }

            return InternalEstablish(config.GetHost(), config.GetPort(), config.GetCache());
        }

        void Connection::Establish(const std::string& host, uint16_t port, const std::string& cache)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(host, port, cache));
        }

        SqlResult Connection::InternalEstablish(const std::string & host, uint16_t port, const std::string & cache)
        {
            if (connected)
            {
                AddStatusRecord(SQL_STATE_08002_ALREADY_CONNECTED, "Already connected.");

                return SQL_RESULT_ERROR;
            }

            this->cache = cache;

            connected = socket.Connect(host.c_str(), port);

            if (!connected)
            {
                AddStatusRecord(SQL_STATE_08001_CANNOT_CONNECT, "Failed to establish connection with the host.");

                return SQL_RESULT_ERROR;
            }

            return SQL_RESULT_SUCCESS;
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

        bool Connection::Send(const int8_t* data, size_t len)
        {
            if (!connected)
                return false;

            OdbcProtocolHeader hdr;

            hdr.len = static_cast<int32_t>(len);

            int sent = socket.Send(reinterpret_cast<int8_t*>(&hdr), sizeof(hdr));

            LOG_MSG("Sent: %d\n", sent);

            if (sent != sizeof(hdr))
                return false;

            sent = 0;

            while (sent != len) 
            {
                int res = socket.Send(data + sent, len - sent);

                LOG_MSG("Sent: %d\n", res);

                if (res <= 0)
                    return false;

                sent += res;
            }

            return true;
        }

        bool Connection::Receive(std::vector<int8_t>& msg)
        {
            if (!connected)
                return false;

            msg.clear();

            OdbcProtocolHeader hdr;

            int received = socket.Receive(reinterpret_cast<int8_t*>(&hdr), sizeof(hdr));

            LOG_MSG("Received: %d\n", received);

            if (received != sizeof(hdr))
                return false;

            size_t remain = hdr.len;
            size_t receivedAtAll = 0;

            msg.resize(remain);

            while (remain)
            {
                received = socket.Receive(&msg[receivedAtAll], remain);
                LOG_MSG("Received: %d\n", received);
                LOG_MSG("remain: %d\n", remain);

                if (received <= 0)
                {
                    msg.resize(receivedAtAll);

                    return false;
                }

                remain -= static_cast<size_t>(received);
            }

            return true;
        }

        const std::string& Connection::GetCache() const
        {
            return cache;
        }

        diagnostic::DiagnosticRecord Connection::CreateStatusRecord(SqlState sqlState,
            const std::string& message, int32_t rowNum, int32_t columnNum) const
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
    }
}

