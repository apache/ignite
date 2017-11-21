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

#ifndef _IGNITE_ODBC_CONNECTION
#define _IGNITE_ODBC_CONNECTION

#include <stdint.h>

#include <vector>

#include "ignite/odbc/parser.h"
#include "ignite/odbc/system/socket_client.h"
#include "ignite/odbc/config/connection_info.h"
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/odbc_error.h"

namespace ignite
{
    namespace odbc
    {
        class Statement;

        /**
         * ODBC node connection.
         */
        class Connection : public diagnostic::DiagnosableAdapter
        {
            friend class Environment;
        public:
            /**
             * Operation with timeout result.
             */
            struct OperationResult
            {
                enum T
                {
                    SUCCESS,
                    FAIL,
                    TIMEOUT
                };
            };

            /**
             * Destructor.
             */
            ~Connection();

            /**
             * Get connection info.
             *
             * @return Connection info.
             */
            const config::ConnectionInfo& GetInfo() const;

            /**
             * Get info of any type.
             *
             * @param type Info type.
             * @param buf Result buffer pointer.
             * @param buflen Result buffer length.
             * @param reslen Result value length pointer.
             */
            void GetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen);

            /**
             * Establish connection to ODBC server.
             *
             * @param connectStr Connection string.
             */
            void Establish(const std::string& connectStr);

            /**
             * Establish connection to ODBC server.
             *
             * @param cfg Configuration.
             */
            void Establish(const config::Configuration cfg);

            /**
             * Release established connection.
             *
             * @return Operation result.
             */
            void Release();

            /**
             * Create statement associated with the connection.
             *
             * @return Pointer to valid instance on success and NULL on failure.
             */
            Statement* CreateStatement();

            /**
             * Send data by established connection.
             *
             * @param data Data buffer.
             * @param len Data length.
             * @param timeout Timeout.
             * @return @c true on success, @c false on timeout.
             * @throw OdbcError on error.
             */
            bool Send(const int8_t* data, size_t len, int32_t timeout);

            /**
             * Receive next message.
             *
             * @param msg Buffer for message.
             * @param timeout Timeout.
             * @return @c true on success, @c false on timeout.
             * @throw OdbcError on error.
             */
            bool Receive(std::vector<int8_t>& msg, int32_t timeout);

            /**
             * Get name of the assotiated schema.
             *
             * @return Schema name.
             */
            const std::string& GetSchema() const;

            /**
             * Get configuration.
             *
             * @return Connection configuration.
             */
            const config::Configuration& GetConfiguration() const;

            /**
             * Create diagnostic record associated with the Connection instance.
             *
             * @param sqlState SQL state.
             * @param message Message.
             * @param rowNum Associated row number.
             * @param columnNum Associated column number.
             * @return DiagnosticRecord associated with the instance.
             */
            static diagnostic::DiagnosticRecord CreateStatusRecord(SqlState::Type sqlState,
                const std::string& message, int32_t rowNum = 0, int32_t columnNum = 0);

            /**
             * Synchronously send request message and receive response.
             * Uses provided timeout.
             *
             * @param req Request message.
             * @param rsp Response message.
             * @param timeout Timeout.
             * @return @c true on success, @c false on timeout.
             * @throw OdbcError on error.
             */
            template<typename ReqT, typename RspT>
            bool SyncMessage(const ReqT& req, RspT& rsp, int32_t timeout)
            {
                std::vector<int8_t> tempBuffer;

                parser.Encode(req, tempBuffer);

                bool success = Send(tempBuffer.data(), tempBuffer.size(), timeout);

                if (!success)
                    return false;

                success = Receive(tempBuffer, timeout);

                if (!success)
                    return false;

                parser.Decode(rsp, tempBuffer);

                return true;
            }

            /**
             * Synchronously send request message and receive response.
             * Uses connection timeout.
             *
             * @param req Request message.
             * @param rsp Response message.
             * @throw OdbcError on error.
             */
            template<typename ReqT, typename RspT>
            void SyncMessage(const ReqT& req, RspT& rsp)
            {
                std::vector<int8_t> tempBuffer;

                parser.Encode(req, tempBuffer);

                bool success = Send(tempBuffer.data(), tempBuffer.size(), timeout);

                if (!success)
                    throw OdbcError(SqlState::SHYT01_CONNECTION_TIMEOUT, "Send operation timed out");

                success = Receive(tempBuffer, timeout);

                if (!success)
                    throw OdbcError(SqlState::SHYT01_CONNECTION_TIMEOUT, "Receive operation timed out");

                parser.Decode(rsp, tempBuffer);
            }

            /**
             * Perform transaction commit.
             */
            void TransactionCommit();

            /**
             * Perform transaction rollback.
             */
            void TransactionRollback();

            /**
             * Get connection attribute.
             *
             * @param attr Attribute type.
             * @param buf Buffer for value.
             * @param bufLen Buffer length.
             * @param valueLen Resulting value length.
             */
            void GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER *valueLen);

            /**
             * Set connection attribute.
             *
             * @param attr Attribute type.
             * @param value Value pointer.
             * @param valueLen Value length.
             */
            void SetAttribute(int attr, void* value, SQLINTEGER valueLen);

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Connection);

            /**
             * Establish connection to ODBC server.
             * Internal call.
             *
             * @param connectStr Connection string.
             * @return Operation result.
             */
            SqlResult::Type InternalEstablish(const std::string& connectStr);

            /**
             * Establish connection to ODBC server.
             * Internal call.
             *
             * @param cfg Configuration.
             * @return Operation result.
             */
            SqlResult::Type InternalEstablish(const config::Configuration cfg);

            /**
             * Release established connection.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult::Type InternalRelease();

            /**
             * Close connection.
             */
            void Close();

            /**
             * Get info of any type.
             * Internal call.
             *
             * @param type Info type.
             * @param buf Result buffer pointer.
             * @param buflen Result buffer length.
             * @param reslen Result value length pointer.
             * @return Operation result.
             */
            SqlResult::Type InternalGetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen);

            /**
             * Create statement associated with the connection.
             * Internal call.
             *
             * @param statement Pointer to valid instance on success and NULL on failure.
             * @return Operation result.
             */
            SqlResult::Type InternalCreateStatement(Statement*& statement);

            /**
             * Perform transaction commit on all the associated connections.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult::Type InternalTransactionCommit();

            /**
             * Perform transaction rollback on all the associated connections.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult::Type InternalTransactionRollback();

            /**
             * Get connection attribute.
             * Internal call.
             *
             * @param attr Attribute type.
             * @param buf Buffer for value.
             * @param bufLen Buffer length.
             * @param valueLen Resulting value length.
             * @return Operation result.
             */
            SqlResult::Type InternalGetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen);

            /**
             * Set connection attribute.
             * Internal call.
             *
             * @param attr Attribute type.
             * @param value Value pointer.
             * @param valueLen Value length.
             * @return Operation result.
             */
            SqlResult::Type InternalSetAttribute(int attr, void* value, SQLINTEGER valueLen);

            /**
             * Receive specified number of bytes.
             *
             * @param dst Buffer for data.
             * @param len Number of bytes to receive.
             * @param timeout Timeout.
             * @return Operation result.
             */
            OperationResult::T ReceiveAll(void* dst, size_t len, int32_t timeout);

            /**
             * Send specified number of bytes.
             *
             * @param data Data buffer.
             * @param len Data length.
             * @param timeout Timeout.
             * @return Operation result.
             */
            OperationResult::T SendAll(const int8_t* data, size_t len, int32_t timeout);

            /**
             * Perform handshake request.
             *
             * @return Operation result.
             */
            SqlResult::Type MakeRequestHandshake();

            /**
             * Constructor.
             */
            Connection();

            /** Socket. */
            tcp::SocketClient socket;

            /** State flag. */
            bool connected;

            /** Connection timeout in seconds. */
            int32_t timeout;

            /** Message parser. */
            Parser parser;

            /** Configuration. */
            config::Configuration config;

            /** Connection info. */
            config::ConnectionInfo info;
        };
    }
}

#endif //_IGNITE_ODBC_CONNECTION