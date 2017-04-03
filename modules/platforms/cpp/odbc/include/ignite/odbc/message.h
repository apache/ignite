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

#include "ignite/odbc/utility.h"
#include "ignite/odbc/result_page.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/meta/table_meta.h"
#include "ignite/odbc/app/parameter.h"

namespace ignite
{
    namespace odbc
    {
        enum RequestType
        {
            REQUEST_TYPE_HANDSHAKE = 1,

            REQUEST_TYPE_EXECUTE_SQL_QUERY = 2,

            REQUEST_TYPE_FETCH_SQL_QUERY = 3,

            REQUEST_TYPE_CLOSE_SQL_QUERY = 4,

            REQUEST_TYPE_GET_COLUMNS_METADATA = 5,

            REQUEST_TYPE_GET_TABLES_METADATA = 6,

            REQUEST_TYPE_GET_PARAMS_METADATA = 7
        };

        enum ResponseStatus
        {
            RESPONSE_STATUS_SUCCESS = 0,

            RESPONSE_STATUS_FAILED = 1
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
             */
            HandshakeRequest(int64_t version, bool distributedJoins, bool enforceJoinOrder) :
                version(version),
                distributedJoins(distributedJoins),
                enforceJoinOrder(enforceJoinOrder)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~HandshakeRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_HANDSHAKE);

                writer.WriteInt64(version);

                writer.WriteBool(distributedJoins);
                writer.WriteBool(enforceJoinOrder);
            }

        private:
            /** Protocol version. */
            int64_t version;

            /** Distributed joins flag. */
            bool distributedJoins;

            /** Enforce join order flag. */
            bool enforceJoinOrder;
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
             * @param cache Cache name.
             * @param sql SQL query.
             * @param params Query arguments.
             */
            QueryExecuteRequest(const std::string& cache, const std::string& sql,
                const app::ParameterBindingMap& params) :
                cache(cache),
                sql(sql),
                params(params)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryExecuteRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_EXECUTE_SQL_QUERY);
                utility::WriteString(writer, cache);
                utility::WriteString(writer, sql);

                writer.WriteInt32(static_cast<int32_t>(params.size()));

                app::ParameterBindingMap::const_iterator i;
                uint16_t prev = 0;

                for (i = params.begin(); i != params.end(); ++i) {
                    uint16_t current = i->first;

                    while ((current - prev) > 1) {
                        writer.WriteNull();
                        ++prev;
                    }

                    i->second.Write(writer);

                    prev = current;
                }
            }

        private:
            /** Cache name. */
            std::string cache;

            /** SQL query. */
            std::string sql;

            /** Parameters bindings. */
            const app::ParameterBindingMap& params;
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
            QueryCloseRequest(int64_t queryId) : queryId(queryId)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryCloseRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_CLOSE_SQL_QUERY);
                writer.WriteInt64(queryId);
            }

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
            QueryFetchRequest(int64_t queryId, int32_t pageSize) :
                queryId(queryId),
                pageSize(pageSize)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryFetchRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_FETCH_SQL_QUERY);
                writer.WriteInt64(queryId);
                writer.WriteInt32(pageSize);
            }

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
            QueryGetColumnsMetaRequest(const std::string& schema, const std::string& table, const std::string& column) :
                schema(schema),
                table(table),
                column(column)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryGetColumnsMetaRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_GET_COLUMNS_METADATA);
                
                utility::WriteString(writer, schema);
                utility::WriteString(writer, table);
                utility::WriteString(writer, column);
            }

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
                                      const std::string& table, const std::string& tableTypes) :
                catalog(catalog),
                schema(schema),
                table(table),
                tableTypes(tableTypes)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryGetTablesMetaRequest()
            {
                // No-op.
            }

            /**
             * Write request using provided writer.
             * @param writer Writer.
             */
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_GET_TABLES_METADATA);

                utility::WriteString(writer, catalog);
                utility::WriteString(writer, schema);
                utility::WriteString(writer, table);
                utility::WriteString(writer, tableTypes);
            }

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
             * @param catalog Catalog search pattern.
             * @param schema Schema search pattern.
             * @param table Table search pattern.
             * @param tableTypes Table types search pattern.
             */
            QueryGetParamsMetaRequest(const std::string& cacheName, const std::string& sqlQuery) :
                cacheName(cacheName),
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
            void Write(ignite::impl::binary::BinaryWriterImpl& writer) const
            {
                writer.WriteInt8(REQUEST_TYPE_GET_PARAMS_METADATA);

                utility::WriteString(writer, cacheName);
                utility::WriteString(writer, sqlQuery);
            }

        private:
            /** Cache name. */
            std::string cacheName;

            /** SQL query. */
            std::string sqlQuery;
        };

        /**
         * Query close response.
         */
        class Response
        {
        public:
            /**
             * Constructor.
             */
            Response() : status(RESPONSE_STATUS_FAILED), error()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            virtual ~Response()
            {
                // No-op.
            }

            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            void Read(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                status = reader.ReadInt8();

                if (status == RESPONSE_STATUS_SUCCESS)
                    ReadOnSuccess(reader);
                else
                    utility::ReadString(reader, error);;
            }
            
            /**
             * Get request processing status.
             * @return Status.
             */
            int8_t GetStatus() const
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
             * Read data if response status is RESPONSE_STATUS_SUCCESS.
             */
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl&)
            {
                // No-op.
            }

        private:
            /** Request processing status. */
            int8_t status;

            /** Error message. */
            std::string error;
        };

        /**
         * Handshake response.
         */
        class HandshakeResponse : public Response
        {
        public:
            /**
             * Constructor.
             */
            HandshakeResponse() :
                accepted(false),
                protoVerSince(),
                currentVer()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~HandshakeResponse()
            {
                // No-op.
            }

            /**
             * Check if the handshake has been accepted.
             * @return True if the handshake has been accepted.
             */
            bool IsAccepted() const
            {
                return accepted;
            }

            /**
             * Get host Apache Ignite version when protocol version has been introduced.
             * @return Host Apache Ignite version when protocol version has been introduced.
             */
            const std::string& ProtoVerSince() const
            {
                return protoVerSince;
            }

            /**
             * Current host Apache Ignite version.
             * @return Current host Apache Ignite version.
             */
            const std::string& CurrentVer() const
            {
                return currentVer;
            }

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                accepted = reader.ReadBool();

                if (!accepted)
                {
                    utility::ReadString(reader, protoVerSince);
                    utility::ReadString(reader, currentVer);
                }
            }

            /** Handshake accepted. */
            bool accepted;

            /** Host Apache Ignite version when protocol version has been introduced. */
            std::string protoVerSince;

            /** Current host Apache Ignite version. */
            std::string currentVer;
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
            QueryCloseResponse() : queryId(0)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryCloseResponse()
            {
                // No-op.
            }

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
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                queryId = reader.ReadInt64();
            }

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
            QueryExecuteResponse() : queryId(0), meta()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryExecuteResponse()
            {
                // No-op.
            }

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

        private:
            /**
             * Read response using provided reader.
             * @param reader Reader.
             */
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                queryId = reader.ReadInt64();

                meta::ReadColumnMetaVector(reader, meta);
            }

            /** Query ID. */
            int64_t queryId;

            /** Columns metadata. */
            meta::ColumnMetaVector meta;
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
            QueryFetchResponse(ResultPage& resultPage) : queryId(0), resultPage(resultPage)
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryFetchResponse()
            {
                // No-op.
            }

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
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                queryId = reader.ReadInt64();

                resultPage.Read(reader);
            }

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
            QueryGetColumnsMetaResponse()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryGetColumnsMetaResponse()
            {
                // No-op.
            }

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
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                meta::ReadColumnMetaVector(reader, meta);
            }

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
            QueryGetTablesMetaResponse()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryGetTablesMetaResponse()
            {
                // No-op.
            }

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
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                meta::ReadTableMetaVector(reader, meta);
            }

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
            QueryGetParamsMetaResponse()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~QueryGetParamsMetaResponse()
            {
                // No-op.
            }

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
            virtual void ReadOnSuccess(ignite::impl::binary::BinaryReaderImpl& reader)
            {
                utility::ReadByteArray(reader, typeIds);
            }

            /** Columns metadata. */
            std::vector<int8_t> typeIds;
        };
    }
}

#endif //_IGNITE_ODBC_MESSAGE
