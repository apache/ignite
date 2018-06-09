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

#include <ignite/impl/thin/response_status.h>
#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/readable.h>

#include <ignite/impl/thin/message.h>
#include "..\..\include\ignite\impl\thin\message.h"
#include "ignite/binary/binary_raw_reader.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            GetOrCreateCacheWithNameRequest::GetOrCreateCacheWithNameRequest(const std::string& name) :
                name(name)
            {
                // No-op.
            }

            void GetOrCreateCacheWithNameRequest::Write(binary::BinaryWriterImpl& writer,
                const ProtocolVersion&) const
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
                status = reader.ReadInt32();

                if (status == ResponseStatus::SUCCESS)
                    ReadOnSuccess(reader, ver);
                else
                    reader.ReadString(error);
            }

            CachePutRequest::CachePutRequest(int32_t cacheId, bool binary, const Writable& key, const Writable& value) :
                cacheId(cacheId),
                binary(binary),
                key(key),
                value(value)
            {
                // No-op.
            }

            void CachePutRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt32(cacheId);
                writer.WriteBool(binary);

                key.Write(writer);
                value.Write(writer);
            }

            CacheGetResponse::CacheGetResponse(Readable& value) :
                value(value)
            {
                // No-op.
            }

            CacheGetResponse::~CacheGetResponse()
            {
                // No-op.
            }

            void CacheGetResponse::ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&)
            {
                value.Read(reader);
            }

            void BinaryTypeGetRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
            {
                writer.WriteInt32(typeId);
            }

            void BinaryTypePutRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
            {
                writer.WriteInt32(snapshot.GetTypeId());
                writer.WriteString(snapshot.GetTypeName());

                // Affinity Key Field name.
                writer.WriteNull();

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

                // Unused for now.
                std::string affKeyFieldNameUnused;
                reader.ReadString(affKeyFieldNameUnused);

                snapshot = binary::SPSnap(new binary::Snap(typeName, typeId));

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
        }
    }
}

