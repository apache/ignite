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

#include <ignite/binary/binary_raw_reader.h>
#include <ignite/thin/cache/cache_peek_mode.h>

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/readable.h>

#include "impl/response_status.h"
#include "impl/data_channel.h"
#include "impl/message.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Message flags.
             */
            struct Flag
            {
                enum Type
                {
                    /** Failure flag. */
                    FAILURE = 1,

                    /** Affinity topology change flag. */
                    AFFINITY_TOPOLOGY_CHANGED = 1 << 1,

                    /** Server notification flag. */
                    NOTIFICATION = 1 << 2
                };
            };

            CachePartitionsRequest::CachePartitionsRequest(const std::vector<int32_t>& cacheIds) :
                cacheIds(cacheIds)
            {
                // No-op.
            }

            void CachePartitionsRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt32(static_cast<int32_t>(cacheIds.size()));

                for (size_t i = 0; i < cacheIds.size(); ++i)
                    writer.WriteInt32(cacheIds[i]);
            }

            GetOrCreateCacheWithNameRequest::GetOrCreateCacheWithNameRequest(const std::string& name) :
                name(name)
            {
                // No-op.
            }

            void GetOrCreateCacheWithNameRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteString(name);
            }

            CreateCacheWithNameRequest::CreateCacheWithNameRequest(const std::string& name) :
                name(name)
            {
                // No-op.
            }

            void CreateCacheWithNameRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteString(name);
            }

            Response::Response():
                flags(),
                status(ResponseStatus::FAILED)
            {
                // No-op.
            }

            Response::~Response()
            {
                // No-op.
            }

            void Response::Read(binary::BinaryReaderImpl& reader, const ProtocolVersion& ver)
            {
                if (ver >= DataChannel::VERSION_1_4_0)
                {
                    flags = reader.ReadInt16();

                    if (IsAffinityTopologyChanged())
                        topologyVersion.Read(reader);

                    if (!IsFailure())
                    {
                        status = ResponseStatus::SUCCESS;

                        ReadOnSuccess(reader, ver);

                        return;
                    }
                }

                status = reader.ReadInt32();

                if (status == ResponseStatus::SUCCESS)
                    ReadOnSuccess(reader, ver);
                else
                    reader.ReadString(error);
            }

            bool Response::IsAffinityTopologyChanged() const
            {
                return (flags & Flag::AFFINITY_TOPOLOGY_CHANGED) != 0;
            }

            bool Response::IsFailure() const
            {
                return (flags & Flag::FAILURE) != 0;
            }

            ClientCacheNodePartitionsResponse::ClientCacheNodePartitionsResponse(
                std::vector<NodePartitions>& nodeParts):
                nodeParts(nodeParts)
            {
                // No-op.
            }

            ClientCacheNodePartitionsResponse::~ClientCacheNodePartitionsResponse()
            {
                // No-op.
            }

            void ClientCacheNodePartitionsResponse::ReadOnSuccess(
                binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                int32_t num = reader.ReadInt32();

                nodeParts.clear();
                nodeParts.resize(static_cast<size_t>(num));

                for (int32_t i = 0; i < num; ++i)
                    nodeParts[i].Read(reader);
            }

            CachePartitionsResponse::CachePartitionsResponse(std::vector<PartitionAwarenessGroup>& groups) :
                groups(groups)
            {
                // No-op.
            }

            CachePartitionsResponse::~CachePartitionsResponse()
            {
                // No-op.
            }

            void CachePartitionsResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                topologyVersion.Read(reader);

                int32_t groupsNum = reader.ReadInt32();

                groups.clear();
                groups.resize(static_cast<size_t>(groupsNum));

                for (int32_t i = 0; i < groupsNum; ++i)
                    groups[i].Read(reader);
            }

            CacheValueResponse::CacheValueResponse(Readable& value) :
                value(value)
            {
                // No-op.
            }

            CacheValueResponse::~CacheValueResponse()
            {
                // No-op.
            }

            void CacheValueResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                value.Read(reader);
            }

            void BinaryTypeGetRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt32(typeId);
            }

            void BinaryTypePutRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt32(snapshot.GetTypeId());
                writer.WriteString(snapshot.GetTypeName());

                const std::string& affFieldName = snapshot.GetAffinityFieldName();
                if (affFieldName.empty())
                    writer.WriteNull();
                else
                    writer.WriteString(affFieldName);

                const binary::Snap::FieldMap& fields = snapshot.GetFieldMap();

                writer.WriteInt32(static_cast<int32_t>(fields.size()));

                for (binary::Snap::FieldMap::const_iterator it = fields.begin(); it != fields.end(); ++it)
                {
                    writer.WriteString(it->first);
                    writer.WriteInt32(it->second.GetTypeId());
                    writer.WriteInt32(it->second.GetFieldId());
                }

                // Is enum: always false for now as we do not support enums.
                writer.WriteBool(false);

                // Schemas. Compact schema is not supported for now.
                writer.WriteInt32(0);
            }

            void BinaryTypeGetResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                int32_t typeId = reader.ReadInt32();

                std::string typeName;
                reader.ReadString(typeName);

                std::string affKeyFieldName;
                reader.ReadString(affKeyFieldName);

                snapshot = binary::SPSnap(new binary::Snap(typeName, affKeyFieldName, typeId));

                int32_t fieldsNum = reader.ReadInt32();

                for (int32_t i = 0; i < fieldsNum; ++i)
                {
                    std::string fieldName;
                    reader.ReadString(fieldName);

                    int32_t fieldTypeId = reader.ReadInt32();
                    int32_t fieldId = reader.ReadInt32();

                    snapshot.Get()->AddField(fieldId, fieldName, fieldTypeId);
                }

                // Check if the type is enum.
                bool isEnum = reader.ReadBool();

                if (isEnum)
                    throw IgniteError(IgniteError::IGNITE_ERR_BINARY, "Enum types is not supported.");

                // Ignoring schemas for now.
            }

            void DestroyCacheRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt32(cacheId);
            }

            void GetCacheNamesResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                int32_t len = reader.ReadInt32();

                cacheNames.reserve(static_cast<size_t>(len));

                for (int32_t i = 0; i < len; i++)
                {
                    std::string res;
                    reader.ReadString(res);

                    cacheNames.push_back(res);
                }
            }

            void BoolResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                value = reader.ReadBool();
            }

            CacheGetSizeRequest::CacheGetSizeRequest(int32_t cacheId, bool binary, int32_t peekModes) :
                CacheRequest<RequestType::CACHE_GET_SIZE>(cacheId, binary),
                peekModes(peekModes)
            {
                // No-op.
            }

            void CacheGetSizeRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
            {
                CacheRequest<RequestType::CACHE_GET_SIZE>::Write(writer, ver);

                if (peekModes & ignite::thin::cache::CachePeekMode::ALL)
                {
                    // Size.
                    writer.WriteInt32(1);

                    writer.WriteInt8(0);

                    return;
                }

                interop::InteropOutputStream* stream = writer.GetStream();

                // Reserve size.
                int32_t sizePos = stream->Reserve(4);

                if (peekModes & ignite::thin::cache::CachePeekMode::NEAR_CACHE)
                    stream->WriteInt8(1);

                if (peekModes & ignite::thin::cache::CachePeekMode::PRIMARY)
                    stream->WriteInt8(2);

                if (peekModes & ignite::thin::cache::CachePeekMode::BACKUP)
                    stream->WriteInt8(3);

                if (peekModes & ignite::thin::cache::CachePeekMode::ONHEAP)
                    stream->WriteInt8(4);

                if (peekModes & ignite::thin::cache::CachePeekMode::OFFHEAP)
                    stream->WriteInt8(5);

                int32_t size = stream->Position() - sizePos - 4;

                stream->WriteInt32(sizePos, size);

                stream->Synchronize();
            }

            void Int64Response::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                value = reader.ReadInt64();
            }

            void Int32Response::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                value = reader.ReadInt32();
            }

            SqlFieldsQueryRequest::SqlFieldsQueryRequest(
                int32_t cacheId,
                const ignite::thin::cache::query::SqlFieldsQuery &qry
                ) :
                CacheRequest<RequestType::QUERY_SQL_FIELDS>(cacheId, false),
                qry(qry)
            {
                // No-op.
            }

            void SqlFieldsQueryRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
            {
                CacheRequest<RequestType::QUERY_SQL_FIELDS>::Write(writer, ver);

                if (qry.schema.empty())
                    writer.WriteNull();
                else
                    writer.WriteString(qry.schema);

                writer.WriteInt32(qry.pageSize);
                writer.WriteInt32(qry.maxRows);
                writer.WriteString(qry.sql);
                writer.WriteInt32(static_cast<int32_t>(qry.args.size()));

                std::vector<impl::thin::CopyableWritable*>::const_iterator it;

                for (it = qry.args.begin(); it != qry.args.end(); ++it)
                    (*it)->Write(writer);

                writer.WriteInt8(0); // Statement type - Any

                writer.WriteBool(qry.distributedJoins);
                writer.WriteBool(qry.loc);
                writer.WriteBool(false); // Replicated only
                writer.WriteBool(qry.enforceJoinOrder);
                writer.WriteBool(qry.collocated);
                writer.WriteBool(qry.lazy);
                writer.WriteInt64(qry.timeout);
                writer.WriteBool(true); // Include field names
            }

            void SqlFieldsQueryResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                ignite::binary::BinaryRawReader rawReader(&reader);

                cursorId = rawReader.ReadInt64();

                int32_t columnsCnt = rawReader.ReadInt32();

                columns.reserve(static_cast<size_t>(columnsCnt));

                for (int32_t i = 0; i < columnsCnt; ++i)
                {
                    columns.push_back(rawReader.ReadString());
                }

                cursorPage.Get()->Read(reader);
            }

            void SqlFieldsCursorGetPageRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt64(cursorId);
            }

            void SqlFieldsCursorGetPageResponse::ReadOnSuccess(binary::BinaryReaderImpl&reader, const ProtocolVersion&)
            {
                cursorPage.Get()->Read(reader);
            }

            void ComputeTaskExecuteRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                // To be changed when Cluster API is implemented.
                int32_t nodesNum = 0;

                writer.WriteInt32(nodesNum);
                writer.WriteInt8(flags);
                writer.WriteInt64(timeout);
                writer.WriteString(taskName);
                arg.Write(writer);
            }

            void ComputeTaskExecuteResponse::ReadOnSuccess(binary::BinaryReaderImpl&reader, const ProtocolVersion&)
            {
                taskId = reader.ReadInt64();
            }

            void ComputeTaskFinishedNotification::Read(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                int16_t flags = reader.ReadInt16();
                if (!(flags & Flag::NOTIFICATION))
                {
                    IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_GENERIC, "Was expecting notification but got "
                        "different kind of message", "flags", flags)
                }

                int16_t opCode = reader.ReadInt16();
                if (opCode != RequestType::COMPUTE_TASK_FINISHED)
                {
                    IGNITE_ERROR_FORMATTED_2(IgniteError::IGNITE_ERR_GENERIC, "Unexpected notification type",
                        "expected", (int)RequestType::COMPUTE_TASK_FINISHED, "actual", opCode)
                }

                if (flags & Flag::FAILURE)
                {
                    status = reader.ReadInt32();
                    reader.ReadString(errorMessage);
                }
                else
                {
                    result.Read(reader);
                }
            }
        }
    }
}

