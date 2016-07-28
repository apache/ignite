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
            /** ODBC communication protocol version. */
            enum { PROTOCOL_VERSION = 1 };

            /**
             * Apache Ignite version when the current ODBC communication
             * protocol version has been introduced.
             */
            static const std::string PROTOCOL_VERSION_SINCE;

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
             */
            void Send(const int8_t* data, size_t len);

            /**
             * Receive next message.
             *
             * @param msg Buffer for message.
             */
            void Receive(std::vector<int8_t>& msg);

            /**
             * Get name of the assotiated cache.
             *
             * @return Cache name.
             */
            const std::string& GetCache() const;

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
            static diagnostic::DiagnosticRecord CreateStatusRecord(SqlState sqlState,
                const std::string& message, int32_t rowNum = 0, int32_t columnNum = 0);

            /**
             * Synchronously send request message and receive response.
             *
             * @param req Request message.
             * @param rsp Response message.
             */
            template<typename ReqT, typename RspT>
            void SyncMessage(const ReqT& req, RspT& rsp)
            {
                std::vector<int8_t> tempBuffer;

                parser.Encode(req, tempBuffer);

                Send(tempBuffer.data(), tempBuffer.size());

                Receive(tempBuffer);

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

        private:
            IGNITE_NO_COPY_ASSIGNMENT(Connection);

            /**
             * Establish connection to ODBC server.
             * Internal call.
             *
             * @param connectStr Connection string.
             * @return Operation result.
             */
            SqlResult InternalEstablish(const std::string& connectStr);

            /**
             * Establish connection to ODBC server.
             * Internal call.
             *
             * @param cfg Configuration.
             * @return Operation result.
             */
            SqlResult InternalEstablish(const config::Configuration cfg);

            /**
             * Release established connection.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult InternalRelease();

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
            SqlResult InternalGetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen);

            /**
             * Create statement associated with the connection.
             * Internal call.
             *
             * @param Pointer to valid instance on success and NULL on failure.
             * @return Operation result.
             */
            SqlResult InternalCreateStatement(Statement*& statement);

            /**
             * Perform transaction commit on all the associated connections.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult InternalTransactionCommit();

            /**
             * Perform transaction rollback on all the associated connections.
             * Internal call.
             *
             * @return Operation result.
             */
            SqlResult InternalTransactionRollback();

            /**
             * Receive specified number of bytes.
             *
             * @param dst Buffer for data.
             * @param len Number of bytes to receive.
             * @return Number of successfully received bytes.
             */
            size_t ReceiveAll(void* dst, size_t len);

            /**
             * Send specified number of bytes.
             *
             * @param data Data buffer.
             * @param len Data length.
             * @return Number of successfully sent bytes.
             */
            size_t SendAll(const int8_t* data, size_t len);

            /**
             * Perform handshake request.
             *
             * @return Operation result.
             */
            SqlResult MakeRequestHandshake();

            /**
             * Constructor.
             */
            Connection();

            /** Socket. */
            tcp::SocketClient socket;

            /** State flag. */
            bool connected;

            /** Message parser. */
            Parser parser;

            /** Configuration. */
            config::Configuration config;
        };
    }
}

#endif //_IGNITE_ODBC_CONNECTION