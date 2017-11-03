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

#ifndef _IGNITE_ODBC_MESSAGE
#define _IGNITE_ODBC_MESSAGE

#include <stdint.h>
#include <string>

#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/impl/binary/binary_reader_impl.h"

#include "ignite/odbc/result_page.h"
#include "ignite/odbc/protocol_version.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/meta/table_meta.h"
#include "ignite/odbc/app/parameter_set.h"

namespace ignite
{
    namespace odbc
    {
        struct ClientType
        {
            enum Type
            {
                ODBC = 0
            };
        };

        struct RequestType
        {
            enum Type
            {
                HANDSHAKE = 1,

                EXECUTE_SQL_QUERY = 2,

                FETCH_SQL_QUERY = 3,

                CLOSE_SQL_QUERY = 4,

                GET_COLUMNS_METADATA = 5,

                GET_TABLES_METADATA = 6,

                GET_PARAMS_METADATA = 7,

                EXECUTE_SQL_QUERY_BATCH = 8,

                QUERY_MORE_RESULTS = 9
            };
        };

        /**
         * Handshake request.
         */
        class HandshakeRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param version Protocol version.
             * @param distributedJoins Distributed joins flag.
             * @param enforceJoinOrder Enforce join order flag.
             * @param replicatedOnly Replicated only flag.
             * @param collocated Collocated flag.
             * @param lazy Lazy flag.
             * @param skipReducerOnUpdate Skip reducer on update.
             */
            HandshakeRequest(const ProtocolVersion& version, bool distributedJoins, bool enforceJoinOrder,
                bool replicatedOnly, bool collocated, bool lazy, bool skipReducerOnUpdate);

            /**
             * Destructor.
             */
            ~HandshakeRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Protocol version. */
            ProtocolVersion version;

            /** Distributed joins flag. */
            bool distributedJoins;

            /** Enforce join order flag. */
            bool enforceJoinOrder;

            /** Replicated only flag. */
            bool replicatedOnly;

            /** Collocated flag. */
            bool collocated;

            /** Lazy flag. */
            bool lazy;

            /** Skip reducer on update flag. */
            bool skipReducerOnUpdate;
        };

        /**
         * Query execute request.
         */
        class QueryExecuteRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param schema Schema.
             * @param sql SQL query.
             * @param params Query arguments.
             */
            QueryExecuteRequest(const std::string& schema, const std::string& sql, const app::ParameterSet& params);

            /**
             * Destructor.
             */
            ~QueryExecuteRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Schema name. */
            std::string schema;

            /** SQL query. */
            std::string sql;

            /** Parameters bindings. */
            const app::ParameterSet& params;
        };

        /**
         * Query execute batch request.
         */
        class QueryExecuteBatchtRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param schema Schema.
             * @param sql SQL query.
             * @param params Query arguments.
             * @param begin Beginng of the interval.
             * @param end End of the interval.
             */
            QueryExecuteBatchtRequest(const std::string& schema, const std::string& sql,
                                          const app::ParameterSet& params, SqlUlen begin, SqlUlen end, bool last);

            /**
             * Destructor.
             */
            ~QueryExecuteBatchtRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Schema name. */
            std::string schema;

            /** SQL query. */
            std::string sql;

            /** Parameters bindings. */
            const app::ParameterSet& params;

            /** Beginng of the interval. */
            SqlUlen begin;

            /** End of the interval. */
            SqlUlen end;

            /** Last page flag. */
            bool last;
        };

        /**
         * Query close request.
         */
        class QueryCloseRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param queryId Query ID.
             */
            QueryCloseRequest(int64_t queryId);

            /**
             * Destructor.
             */
            ~QueryCloseRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Query ID. */
            int64_t queryId;
        };

        /**
         * Query fetch request.
         */
        class QueryFetchRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param queryId Query ID.
             * @param pageSize Required page size.
             */
            QueryFetchRequest(int64_t queryId, int32_t pageSize);

            /**
             * Destructor.
             */
            ~QueryFetchRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Query ID. */
            int64_t queryId;

            /** SQL query. */
            int32_t pageSize;
        };

        /**
         * Query get columns metadata request.
         */
        class QueryGetColumnsMetaRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param schema Schema name.
             * @param table Table name.
             * @param column Column name.
             */
            QueryGetColumnsMetaRequest(const std::string& schema, const std::string& table, const std::string& column);

            /**
             * Destructor.
             */
            ~QueryGetColumnsMetaRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Schema search pattern. */
            std::string schema;

            /** Table search pattern. */
            std::string table;

            /** Column search pattern. */
            std::string column;
        };

        /**
         * Query get tables metadata request.
         */
        class QueryGetTablesMetaRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param catalog Catalog search pattern.
             * @param schema Schema search pattern.
             * @param table Table search pattern.
             * @param tableTypes Table types search pattern.
             */
            QueryGetTablesMetaRequest(const std::string& catalog, const std::string& schema,
                                      const std::string& table, const std::string& tableTypes);

            /**
             * Destructor.
             */
            ~QueryGetTablesMetaRequest();

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Column search pattern. */
            std::string catalog;

            /** Schema search pattern. */
            std::string schema;

            /** Table search pattern. */
            std::string table;

            /** Column search pattern. */
            std::string tableTypes;
        };

        /**
         * Get parameter metadata request.
         */
        class QueryGetParamsMetaRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param schema Schema.
             * @param sqlQuery SQL query itself.
             */
            QueryGetParamsMetaRequest(const std::string& schema, const std::string& sqlQuery) :
                schema(schema),
                sqlQuery(sqlQuery)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryGetParamsMetaRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Schema. */
            std::string schema;

            /** SQL query. */
            std::string sqlQuery;
        };

        /**
         * Query fetch request.
         */
        class QueryMoreResultsRequest
        {
        public:
            /**
             * Constructor.
             *
             * @param queryId Query ID.
             * @param pageSize Required page size.
             */
            QueryMoreResultsRequest(int64_t queryId, int32_t pageSize) :
                queryId(queryId),
                pageSize(pageSize)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryMoreResultsRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(impl::binary::BinaryWriterImpl& writer) const;

        private:
            /** Query ID. */
            int64_t queryId;

            /** SQL query. */
            int32_t pageSize;
        };

        /**
         * General response.
         */
        class Response
        {
        public:
            /**
             * Constructor.
             */
            Response();

            /**
             * Destructor.
             */
            virtual ~Response();

            /**
             * Read response using provided reader.
             * @param reader Reader.
             * @param ver Protocol version.
             */
            void Read(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

            /**
             * Get request processing status.
             * @return Status.
             */
            int32_t GetStatus() const
            {
                return status;
            }

            /**
             * Get resulting error.
             * @return Error.
             */
            const std::string& GetError() const
            {
                return error;
            }

        protected:
            /**
             * Read data if response status is ResponseStatus::SUCCESS.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl&, const ProtocolVersion&);

        private:
            /** Request processing status. */
            int32_t status;

            /** Error message. */
            std::string error;
        };

        /**
         * Handshake response.
         */
        class HandshakeResponse
        {
        public:
            /**
             * Constructor.
             */
            HandshakeResponse();

            /**
             * Destructor.
             */
            ~HandshakeResponse();

            /**
             * Check if the handshake has been accepted.
             * @return True if the handshake has been accepted.
             */
            bool IsAccepted() const
            {
                return accepted;
            }

            /**
             * Get optional error.
             * @return Optional error message.
             */
            const std::string& GetError() const
            {
                return error;
            }

            /**
             * Current host Apache Ignite version.
             * @return Current host Apache Ignite version.
             */
            const ProtocolVersion& GetCurrentVer() const
            {
                return currentVer;
            }

            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            void Read(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

        private:
            /** Handshake accepted. */
            bool accepted;

            /** Node's protocol version. */
            ProtocolVersion currentVer;

            /** Optional error message. */
            std::string error;
        };

        /**
         * Query close response.
         */
        class QueryCloseResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            QueryCloseResponse();

            /**
             * Destructor.
             */
            virtual ~QueryCloseResponse();

            /**
             * Get query ID.
             * @return Query ID.
             */
            int64_t GetQueryId() const
            {
                return queryId;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            /** Query ID. */
            int64_t queryId;
        };

        /**
         * Query execute response.
         */
        class QueryExecuteResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            QueryExecuteResponse();

            /**
             * Destructor.
             */
            virtual ~QueryExecuteResponse();

            /**
             * Get query ID.
             * @return Query ID.
             */
            int64_t GetQueryId() const
            {
                return queryId;
            }

            /**
             * Get column metadata.
             * @return Column metadata.
             */
            const meta::ColumnMetaVector& GetMeta() const
            {
                return meta;
            }

            /**
             * Get affected rows number.
             * @return Number of rows affected by the query.
             */
            const std::vector<int64_t>& GetAffectedRows()
            {
                return affectedRows;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

            /** Query ID. */
            int64_t queryId;

            /** Columns metadata. */
            meta::ColumnMetaVector meta;

            /** Number of affected rows. */
            std::vector<int64_t> affectedRows;
        };

        /**
         * Query execute batch start response.
         */
        class QueryExecuteBatchResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            QueryExecuteBatchResponse();

            /**
             * Destructor.
             */
            virtual ~QueryExecuteBatchResponse();

            /**
             * Affected rows.
             * @return Affected rows.
             */
            const std::vector<int64_t>& GetAffectedRows() const
            {
                return affectedRows;
            }

            /**
             * Get index of the set which caused an error.
             * @return Index of the set which caused an error.
             */
            int64_t GetErrorSetIdx() const
            {
                return static_cast<int64_t>(affectedRows.size());
            }

            /**
             * Get error message.
             * @return Error message.
             */
            const std::string& GetErrorMessage() const
            {
                return errorMessage;
            }

            /**
             * Get error code.
             * @return Error code.
             */
            int32_t GetErrorCode() const
            {
                return errorCode;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             * @param ver Protocol version.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

            /** Affected rows. */
            std::vector<int64_t> affectedRows;

            /** Index of the set which caused an error. */
            int64_t errorSetIdx;

            /** Error message. */
            std::string errorMessage;

            /** Error code. */
            int32_t errorCode;
        };

        /**
         * Query fetch response.
         */
        class QueryFetchResponse : public Response
        {
        public:
            /**
             * Constructor.
             * @param resultPage Result page.
             */
            QueryFetchResponse(ResultPage& resultPage);

            /**
             * Destructor.
             */
            virtual ~QueryFetchResponse();

            /**
             * Get query ID.
             * @return Query ID.
             */
            int64_t GetQueryId() const
            {
                return queryId;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            /** Query ID. */
            int64_t queryId;

            /** Result page. */
            ResultPage& resultPage;
        };

        /**
         * Query get column metadata response.
         */
        class QueryGetColumnsMetaResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            QueryGetColumnsMetaResponse();

            /**
             * Destructor.
             */
            virtual ~QueryGetColumnsMetaResponse();

            /**
             * Get column metadata.
             * @return Column metadata.
             */
            const meta::ColumnMetaVector& GetMeta() const
            {
                return meta;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            /** Columns metadata. */
            meta::ColumnMetaVector meta;
        };

        /**
         * Query get table metadata response.
         */
        class QueryGetTablesMetaResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            QueryGetTablesMetaResponse();

            /**
             * Destructor.
             */
            virtual ~QueryGetTablesMetaResponse();

            /**
             * Get column metadata.
             * @return Column metadata.
             */
            const meta::TableMetaVector& GetMeta() const
            {
                return meta;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            /** Columns metadata. */
            meta::TableMetaVector meta;
        };

        /**
         * Get params metadata response.
         */
        class QueryGetParamsMetaResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            QueryGetParamsMetaResponse();

            /**
             * Destructor.
             */
            virtual ~QueryGetParamsMetaResponse();

            /**
             * Get parameter type IDs.
             * @return Type IDs.
             */
            const std::vector<int8_t>& GetTypeIds() const
            {
                return typeIds;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            /** Columns metadata. */
            std::vector<int8_t> typeIds;
        };

        /**
         * Query fetch response.
         */
        class QueryMoreResultsResponse : public Response
        {
        public:
            /**
             * Constructor.
             * @param resultPage Result page.
             */
            QueryMoreResultsResponse(ResultPage& resultPage);

            /**
             * Destructor.
             */
            virtual ~QueryMoreResultsResponse();

            /**
             * Get query ID.
             * @return Query ID.
             */
            int64_t GetQueryId() const
            {
                return queryId;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            /** Query ID. */
            int64_t queryId;

            /** Result page. */
            ResultPage& resultPage;
        };
    }
}

#endif //_IGNITE_ODBC_MESSAGE
