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

#include "ignite/odbc/message.h"
#include "ignite/odbc/utility.h"

namespace ignite
{
    namespace odbc
    {
        HandshakeRequest::HandshakeRequest(int64_t version, bool distributedJoins, bool enforceJoinOrder):
            version(version),
            distributedJoins(distributedJoins),
            enforceJoinOrder(enforceJoinOrder)
        {
            // No-op.
        }

        HandshakeRequest::~HandshakeRequest()
        {
            // No-op.
        }

        void HandshakeRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_HANDSHAKE);

            writer.WriteInt64(version);

            writer.WriteBool(distributedJoins);
            writer.WriteBool(enforceJoinOrder);
        }

        QueryExecuteRequest::QueryExecuteRequest(const std::string& schema, const std::string& sql, const app::ParameterSet& params):
            cache(schema),
            sql(sql),
            params(params)
        {
            // No-op.
        }

        QueryExecuteRequest::~QueryExecuteRequest()
        {
            // No-op.
        }

        void QueryExecuteRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_EXECUTE_SQL_QUERY);

            if (cache.empty())
                writer.WriteNull();
            else
                writer.WriteObject<std::string>(cache);

            writer.WriteObject<std::string>(sql);

            params.Write(writer);
        }

        QueryExecuteBatchtRequest::QueryExecuteBatchtRequest(const std::string& schema, const std::string& sql,
            const app::ParameterSet& params, SqlUlen begin, SqlUlen end, bool last):
            schema(schema),
            sql(sql),
            params(params),
            begin(begin),
            end(end),
            last(last)
        {
            // No-op.
        }

        QueryExecuteBatchtRequest::~QueryExecuteBatchtRequest()
        {
            // No-op.
        }

        void QueryExecuteBatchtRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_EXECUTE_SQL_QUERY_BATCH);

            if (schema.empty())
                writer.WriteNull();
            else
                writer.WriteObject<std::string>(schema);

            writer.WriteObject<std::string>(sql);

            params.Write(writer, begin, end, last);
        }

        QueryCloseRequest::QueryCloseRequest(int64_t queryId): queryId(queryId)
        {
            // No-op.
        }

        QueryCloseRequest::~QueryCloseRequest()
        {
            // No-op.
        }

        void QueryCloseRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_CLOSE_SQL_QUERY);
            writer.WriteInt64(queryId);
        }

        QueryFetchRequest::QueryFetchRequest(int64_t queryId, int32_t pageSize):
            queryId(queryId),
            pageSize(pageSize)
        {
            // No-op.
        }

        QueryFetchRequest::~QueryFetchRequest()
        {
            // No-op.
        }

        void QueryFetchRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_FETCH_SQL_QUERY);
            writer.WriteInt64(queryId);
            writer.WriteInt32(pageSize);
        }

        QueryGetColumnsMetaRequest::QueryGetColumnsMetaRequest(const std::string& schema, const std::string& table, const std::string& column):
            schema(schema),
            table(table),
            column(column)
        {
            // No-op.
        }

        QueryGetColumnsMetaRequest::~QueryGetColumnsMetaRequest()
        {
            // No-op.
        }

        void QueryGetColumnsMetaRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_GET_COLUMNS_METADATA);

            utility::WriteString(writer, schema);
            utility::WriteString(writer, table);
            utility::WriteString(writer, column);
        }

        QueryGetTablesMetaRequest::QueryGetTablesMetaRequest(const std::string& catalog, const std::string& schema, const std::string& table, const std::string& tableTypes):
            catalog(catalog),
            schema(schema),
            table(table),
            tableTypes(tableTypes)
        {
            // No-op.
        }

        QueryGetTablesMetaRequest::~QueryGetTablesMetaRequest()
        {
            // No-op.
        }

        void QueryGetTablesMetaRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_GET_TABLES_METADATA);

            utility::WriteString(writer, catalog);
            utility::WriteString(writer, schema);
            utility::WriteString(writer, table);
            utility::WriteString(writer, tableTypes);
        }

        void QueryGetParamsMetaRequest::Write(impl::binary::BinaryWriterImpl& writer) const
        {
            writer.WriteInt8(REQUEST_TYPE_GET_PARAMS_METADATA);

            utility::WriteString(writer, cacheName);
            utility::WriteString(writer, sqlQuery);
        }

        Response::Response():
            status(RESPONSE_STATUS_FAILED),
            error()
        {
            // No-op.
        }

        Response::~Response()
        {
            // No-op.
        }

        void Response::Read(impl::binary::BinaryReaderImpl& reader)
        {
            status = reader.ReadInt8();

            if (status == RESPONSE_STATUS_SUCCESS)
                ReadOnSuccess(reader);
            else
                utility::ReadString(reader, error);
        }

        void Response::ReadOnSuccess(impl::binary::BinaryReaderImpl&)
        {
            // No-op.
        }

        HandshakeResponse::HandshakeResponse():
            accepted(false),
            protoVerSince(),
            currentVer()
        {
            // No-op.
        }

        HandshakeResponse::~HandshakeResponse()
        {
            // No-op.
        }

        void HandshakeResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            accepted = reader.ReadBool();

            if (!accepted)
            {
                utility::ReadString(reader, protoVerSince);
                utility::ReadString(reader, currentVer);
            }
        }

        QueryCloseResponse::QueryCloseResponse(): queryId(0)
        {
            // No-op.
        }

        QueryCloseResponse::~QueryCloseResponse()
        {
            // No-op.
        }

        void QueryCloseResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            queryId = reader.ReadInt64();
        }

        QueryExecuteResponse::QueryExecuteResponse(): queryId(0), meta()
        {
            // No-op.
        }

        QueryExecuteResponse::~QueryExecuteResponse()
        {
            // No-op.
        }

        void QueryExecuteResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            queryId = reader.ReadInt64();

            meta::ReadColumnMetaVector(reader, meta);
        }

        QueryExecuteBatchResponse::QueryExecuteBatchResponse():
            affectedRows(0),
            errorSetIdx(-1),
            errorMessage()
        {
            // No-op.
        }

        QueryExecuteBatchResponse::~QueryExecuteBatchResponse()
        {
            // No-op.
        }

        void QueryExecuteBatchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            bool success = reader.ReadBool();
            affectedRows = reader.ReadInt64();

            if (!success)
            {
                errorSetIdx = reader.ReadInt64();
                errorMessage = reader.ReadObject<std::string>();
            }
        }

        QueryFetchResponse::QueryFetchResponse(ResultPage& resultPage): queryId(0), resultPage(resultPage)
        {
            // No-op.
        }

        QueryFetchResponse::~QueryFetchResponse()
        {
            // No-op.
        }

        void QueryFetchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            queryId = reader.ReadInt64();

            resultPage.Read(reader);
        }

        QueryGetColumnsMetaResponse::QueryGetColumnsMetaResponse()
        {
            // No-op.
        }

        QueryGetColumnsMetaResponse::~QueryGetColumnsMetaResponse()
        {
            // No-op.
        }

        void QueryGetColumnsMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            meta::ReadColumnMetaVector(reader, meta);
        }

        QueryGetTablesMetaResponse::QueryGetTablesMetaResponse()
        {
            // No-op.
        }

        QueryGetTablesMetaResponse::~QueryGetTablesMetaResponse()
        {
            // No-op.
        }

        void QueryGetTablesMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            meta::ReadTableMetaVector(reader, meta);
        }

        QueryGetParamsMetaResponse::QueryGetParamsMetaResponse()
        {
            // No-op.
        }

        QueryGetParamsMetaResponse::~QueryGetParamsMetaResponse()
        {
            // No-op.
        }

        void QueryGetParamsMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader)
        {
            utility::ReadByteArray(reader, typeIds);
        }
    }
}

