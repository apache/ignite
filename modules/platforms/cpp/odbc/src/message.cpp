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

namespace
{
    using namespace ignite;
    using namespace odbc;

    void ReadAffectedRows(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& protocolVersion,
        std::vector<int64_t>& affectedRows)
    {
        affectedRows.clear();

        if (protocolVersion < ProtocolVersion::VERSION_2_3_2)
            affectedRows.push_back(reader.ReadInt64());
        else
        {
            int32_t len = reader.ReadInt32();

            affectedRows.reserve(static_cast<size_t>(len));
            for (int32_t i = 0; i < len; ++i)
                affectedRows.push_back(reader.ReadInt64());
        }
    }
}

namespace ignite
{
    namespace odbc
    {
        HandshakeRequest::HandshakeRequest(const config::Configuration& config) :
            config(config)
        {
            // No-op.
        }

        HandshakeRequest::~HandshakeRequest()
        {
            // No-op.
        }

        void HandshakeRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::HANDSHAKE);

            ProtocolVersion version = config.GetProtocolVersion();
            writer.WriteInt16(version.GetMajor());
            writer.WriteInt16(version.GetMinor());
            writer.WriteInt16(version.GetMaintenance());

            writer.WriteInt8(ClientType::ODBC);

            writer.WriteBool(config.IsDistributedJoins());
            writer.WriteBool(config.IsEnforceJoinOrder());
            writer.WriteBool(config.IsReplicatedOnly());
            writer.WriteBool(config.IsCollocated());

            if (version >= ProtocolVersion::VERSION_2_1_5)
                writer.WriteBool(config.IsLazy());

            if (version >= ProtocolVersion::VERSION_2_3_0)
                writer.WriteBool(config.IsSkipReducerOnUpdate());

            if (version >= ProtocolVersion::VERSION_2_5_0)
            {
                utility::WriteString(writer, config.GetUser());
                utility::WriteString(writer, config.GetPassword());
            }
        }

        QueryExecuteRequest::QueryExecuteRequest(const std::string& schema, const std::string& sql,
            const app::ParameterSet& params, int32_t timeout):
            schema(schema),
            sql(sql),
            params(params),
            timeout(timeout)
        {
            // No-op.
        }

        QueryExecuteRequest::~QueryExecuteRequest()
        {
            // No-op.
        }

        void QueryExecuteRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
        {
            writer.WriteInt8(RequestType::EXECUTE_SQL_QUERY);

            if (schema.empty())
                writer.WriteNull();
            else
                writer.WriteObject<std::string>(schema);

            writer.WriteObject<std::string>(sql);

            params.Write(writer);

            if (ver >= ProtocolVersion::VERSION_2_3_2)
                writer.WriteInt32(timeout);
        }

        QueryExecuteBatchtRequest::QueryExecuteBatchtRequest(const std::string& schema, const std::string& sql,
            const app::ParameterSet& params, SqlUlen begin, SqlUlen end, bool last, int32_t timeout):
            schema(schema),
            sql(sql),
            params(params),
            begin(begin),
            end(end),
            last(last),
            timeout(timeout)
        {
            // No-op.
        }

        QueryExecuteBatchtRequest::~QueryExecuteBatchtRequest()
        {
            // No-op.
        }

        void QueryExecuteBatchtRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
        {
            writer.WriteInt8(RequestType::EXECUTE_SQL_QUERY_BATCH);

            if (schema.empty())
                writer.WriteNull();
            else
                writer.WriteObject<std::string>(schema);

            writer.WriteObject<std::string>(sql);

            params.Write(writer, begin, end, last);

            if (ver >= ProtocolVersion::VERSION_2_3_2)
                writer.WriteInt32(timeout);
        }

        QueryCloseRequest::QueryCloseRequest(int64_t queryId): queryId(queryId)
        {
            // No-op.
        }

        QueryCloseRequest::~QueryCloseRequest()
        {
            // No-op.
        }

        void QueryCloseRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::CLOSE_SQL_QUERY);
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

        void QueryFetchRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::FETCH_SQL_QUERY);

            writer.WriteInt64(queryId);
            writer.WriteInt32(pageSize);
        }

        QueryGetColumnsMetaRequest::QueryGetColumnsMetaRequest(const std::string& schema, const std::string& table,
            const std::string& column):
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

        void QueryGetColumnsMetaRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::GET_COLUMNS_METADATA);

            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(table);
            writer.WriteObject<std::string>(column);
        }

        QueryGetTablesMetaRequest::QueryGetTablesMetaRequest(const std::string& catalog, const std::string& schema,
            const std::string& table, const std::string& tableTypes):
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

        void QueryGetTablesMetaRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::GET_TABLES_METADATA);

            writer.WriteObject<std::string>(catalog);
            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(table);
            writer.WriteObject<std::string>(tableTypes);
        }

        void QueryGetParamsMetaRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::GET_PARAMS_METADATA);

            writer.WriteObject<std::string>(schema);
            writer.WriteObject<std::string>(sqlQuery);
        }

        void QueryMoreResultsRequest::Write(impl::binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
        {
            writer.WriteInt8(RequestType::QUERY_MORE_RESULTS);

            writer.WriteInt64(queryId);
            writer.WriteInt32(pageSize);
        }

        Response::Response() :
            status(ResponseStatus::UNKNOWN_ERROR),
            error()
        {
            // No-op.
        }

        Response::~Response()
        {
            // No-op.
        }

        void Response::Read(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver)
        {
            if (ver < ProtocolVersion::VERSION_2_1_5)
                status = reader.ReadInt8();
            else
                status = reader.ReadInt32();

            if (status == ResponseStatus::SUCCESS)
                ReadOnSuccess(reader, ver);
            else
                utility::ReadString(reader, error);
        }

        void Response::ReadOnSuccess(impl::binary::BinaryReaderImpl&, const ProtocolVersion&)
        {
            // No-op.
        }

        HandshakeResponse::HandshakeResponse():
            accepted(false),
            currentVer(),
            error()
        {
            // No-op.
        }

        HandshakeResponse::~HandshakeResponse()
        {
            // No-op.
        }

        void HandshakeResponse::Read(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
        {
            accepted = reader.ReadBool();

            if (!accepted)
            {
                int16_t major = reader.ReadInt16();
                int16_t minor = reader.ReadInt16();
                int16_t maintenance = reader.ReadInt16();

                currentVer = ProtocolVersion(major, minor, maintenance);

                utility::ReadString(reader, error);
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

        void QueryCloseResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
        {
            queryId = reader.ReadInt64();
        }

        QueryExecuteResponse::QueryExecuteResponse():
            queryId(0),
            meta(),
            affectedRows(0)
        {
            // No-op.
        }

        QueryExecuteResponse::~QueryExecuteResponse()
        {
            // No-op.
        }

        void QueryExecuteResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver)
        {
            queryId = reader.ReadInt64();

            meta::ReadColumnMetaVector(reader, meta);

            ReadAffectedRows(reader, ver, affectedRows);
        }

        QueryExecuteBatchResponse::QueryExecuteBatchResponse():
            affectedRows(0),
            errorMessage(),
            errorCode(1)
        {
            // No-op.
        }

        QueryExecuteBatchResponse::~QueryExecuteBatchResponse()
        {
            // No-op.
        }

        void QueryExecuteBatchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion& ver)
        {
            bool success = reader.ReadBool();

            ReadAffectedRows(reader, ver, affectedRows);

            if (!success)
            {
                // Ignoring error set idx. To be deleted in next major version.
                reader.ReadInt64();
                errorMessage = reader.ReadObject<std::string>();

                if (ver >= ProtocolVersion::VERSION_2_1_5)
                    errorCode = reader.ReadInt32();
            }
        }

        QueryFetchResponse::QueryFetchResponse(ResultPage& resultPage) :
            queryId(0),
            resultPage(resultPage)
        {
            // No-op.
        }

        QueryFetchResponse::~QueryFetchResponse()
        {
            // No-op.
        }

        void QueryFetchResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
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

        void QueryGetColumnsMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
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

        void QueryGetTablesMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
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

        void QueryGetParamsMetaResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
        {
            utility::ReadByteArray(reader, typeIds);
        }

        QueryMoreResultsResponse::QueryMoreResultsResponse(ResultPage & resultPage) :
            queryId(0),
            resultPage(resultPage)
        {
            // No-op.
        }

        QueryMoreResultsResponse::~QueryMoreResultsResponse()
        {
            // No-op.
        }

        void QueryMoreResultsResponse::ReadOnSuccess(impl::binary::BinaryReaderImpl& reader, const ProtocolVersion&)
        {
            queryId = reader.ReadInt64();

            resultPage.Read(reader);
        }
    }
}

