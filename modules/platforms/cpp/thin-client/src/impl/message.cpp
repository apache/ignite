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

            CacheGetRequest::CacheGetRequest(int32_t cacheId, bool binary, const Writable& key) :
                cacheId(cacheId),
                binary(binary),
                key(key)
            {
                // No-op.
            }

            void CacheGetRequest::Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
            {
                writer.WriteInt32(cacheId);
                writer.WriteBool(binary);

                key.Write(writer);
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
        }
    }
}

