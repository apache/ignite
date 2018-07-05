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

#ifndef _IGNITE_IMPL_THIN_MESSAGE
#define _IGNITE_IMPL_THIN_MESSAGE

#include <stdint.h>
#include <string>
#include <vector>

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/readable.h>

#include "impl/connectable_node_partitions.h"
#include "impl/protocol_version.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class Readable;

            /* Forward declaration. */
            class Writable;

            struct ClientType
            {
                enum Type
                {
                    THIN_CLIENT = 2
                };
            };

            struct RequestType
            {
                enum Type
                {
                    /** Resource close. */
                    RESOURCE_CLOSE = 0,
                    
                    /** Handshake. */
                    HANDSHAKE = 1,

                    /** Cache get. */
                    CACHE_GET = 1000,

                    /** Cache put. */
                    CACHE_PUT = 1001,

                    /** Cache put if absent. */
                    CACHE_PUT_IF_ABSENT = 1002,

                    /** Get all. */
                    CACHE_GET_ALL = 1003,

                    /** Put all. */
                    CACHE_PUT_ALL = 1004,

                    /** Cache get and put. */
                    CACHE_GET_AND_PUT = 1005,

                    /** Cache get and replace. */
                    CACHE_GET_AND_REPLACE = 1006,

                    /** Cache get and remove. */
                    CACHE_GET_AND_REMOVE = 1007,

                    /** Cache get and put if absent. */
                    CACHE_GET_AND_PUT_IF_ABSENT = 1008,

                    /** Cache replace. */
                    CACHE_REPLACE = 1009,

                    /** Cache replace if equals. */
                    CACHE_REPLACE_IF_EQUALS = 1010,

                    /** Cache contains key. */
                    CACHE_CONTAINS_KEY = 1011,

                    /** Cache contains keys. */
                    CACHE_CONTAINS_KEYS = 1012,

                    /** Cache clear. */
                    CACHE_CLEAR = 1013,

                    /** Cache clear key. */
                    CACHE_CLEAR_KEY = 1014,

                    /** Cache clear keys. */
                    CACHE_CLEAR_KEYS = 1015,

                    /** Cache remove key. */
                    CACHE_REMOVE_KEY = 1016,

                    /** Cache remove if equals. */
                    CACHE_REMOVE_IF_EQUALS = 1017,

                    /** Cache remove keys. */
                    CACHE_REMOVE_KEYS = 1018,

                    /** Cache remove all. */
                    CACHE_REMOVE_ALL = 1019,

                    /** Get size. */
                    CACHE_GET_SIZE = 1020,

                    /** Local peek. */
                    CACHE_LOCAL_PEEK = 1021,

                    /** Cache get names. */
                    CACHE_GET_NAMES = 1050,

                    /** Cache create with name. */
                    CACHE_CREATE_WITH_NAME = 1051,

                    /** Cache get or create with name. */
                    CACHE_GET_OR_CREATE_WITH_NAME = 1052,

                    /** Cache destroy. */
                    CACHE_DESTROY = 1056,

                    /** Cache nodes and partitions request. */
                    CACHE_NODE_PARTITIONS = 1100,

                    /** Get binary type info. */
                    GET_BINARY_TYPE = 3002,

                    /** Put binary type info. */
                    PUT_BINARY_TYPE = 3003,
                };
            };

            /**
             * Request.
             *
             * @tparam OpCode Operation code.
             */
            template<int32_t OpCode>
            class Request
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~Request()
                {
                    // No-op.
                }

                /**
                 * Get operation code.
                 *
                 * @return Operation code.
                 */
                static int32_t GetOperationCode()
                {
                    return OpCode;
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
                {
                    // No-op.
                }
            };

            /**
             * Get or create cache request.
             */
            class GetOrCreateCacheWithNameRequest : public Request<RequestType::CACHE_GET_OR_CREATE_WITH_NAME>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param name Cache name.
                 */
                GetOrCreateCacheWithNameRequest(const std::string& name);

                /**
                 * Destructor.
                 */
                virtual ~GetOrCreateCacheWithNameRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Name. */
                std::string name;
            };

            /**
             * Get or create cache request.
             */
            class CreateCacheWithNameRequest : public Request<RequestType::CACHE_CREATE_WITH_NAME>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param name Cache name.
                 */
                CreateCacheWithNameRequest(const std::string& name);

                /**
                 * Destructor.
                 */
                virtual ~CreateCacheWithNameRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Name. */
                std::string name;
            };

            /**
             * Destroy cache request.
             */
            class DestroyCacheRequest : public Request<RequestType::CACHE_DESTROY>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 */
                DestroyCacheRequest(int32_t cacheId) :
                    cacheId(cacheId)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~DestroyCacheRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Cache ID. */
                int32_t cacheId;
            };

            /**
             * Cache request.
             *
             * Request to cache.
             */
            template<int32_t OpCode>
            class CacheRequest : public Request<OpCode>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 */
                CacheRequest(int32_t cacheId, bool binary) :
                    cacheId(cacheId),
                    binary(binary)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CacheRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
                {
                    writer.WriteInt32(cacheId);
                    writer.WriteBool(binary);
                }

            private:
                /** Cache ID. */
                int32_t cacheId;

                /** Binary flag. */
                bool binary;
            };

            /**
             * Cache get size request.
             */
            class CacheGetSizeRequest : public CacheRequest<RequestType::CACHE_GET_SIZE>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param peekModes Peek modes.
                 */
                CacheGetSizeRequest(int32_t cacheId, bool binary, int32_t peekModes);

                /**
                 * Destructor.
                 */
                virtual ~CacheGetSizeRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Peek modes. */
                int32_t peekModes;
            };

            /**
             * Cache key request.
             *
             * Request to cache containing single key.
             */
            template<int32_t OpCode>
            class CacheKeyRequest : public CacheRequest<OpCode>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param key Key.
                 */
                CacheKeyRequest(int32_t cacheId, bool binary, const Writable& key) :
                    CacheRequest<OpCode>(cacheId, binary),
                    key(key)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CacheKeyRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const
                {
                    CacheRequest<OpCode>::Write(writer, ver);

                    key.Write(writer);
                }

            private:
                /** Key. */
                const Writable& key;
            };

            /**
             * Cache put request.
             */
            class CachePutRequest : public CacheKeyRequest<RequestType::CACHE_PUT>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param key Key.
                 * @param value Value.
                 */
                CachePutRequest(int32_t cacheId, bool binary, const Writable& key, const Writable& value);

                /**
                 * Destructor.
                 */
                virtual ~CachePutRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Value. */
                const Writable& value;
            };

            /**
             * Cache get binary type request.
             */
            class BinaryTypeGetRequest : public Request<RequestType::GET_BINARY_TYPE>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param typeId Type ID.
                 */
                BinaryTypeGetRequest(int32_t typeId) :
                    typeId(typeId)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~BinaryTypeGetRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Cache ID. */
                int32_t typeId;
            };

            /**
             * Cache put binary type request.
             */
            class BinaryTypePutRequest : public Request<RequestType::PUT_BINARY_TYPE>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param snapshot Type snapshot.
                 */
                BinaryTypePutRequest(const binary::Snap& snapshot) :
                    snapshot(snapshot)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~BinaryTypePutRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Cache ID. */
                const binary::Snap& snapshot;
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
                void Read(binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

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
                virtual void ReadOnSuccess(binary::BinaryReaderImpl&, const ProtocolVersion&)
                {
                    // No-op.
                }

            private:
                /** Request processing status. */
                int32_t status;

                /** Error message. */
                std::string error;
            };

            /**
             * Cache node list request.
             */
            class ClientCacheNodePartitionsResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param nodeParts Node partitions.
                 */
                ClientCacheNodePartitionsResponse(std::vector<ConnectableNodePartitions>& nodeParts);

                /**
                 * Destructor.
                 */
                virtual ~ClientCacheNodePartitionsResponse();

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Node partitions. */
                std::vector<ConnectableNodePartitions>& nodeParts;
            };

            /**
             * Cache get response.
             */
            class CacheGetResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                CacheGetResponse(Readable& value);

                /**
                 * Destructor.
                 */
                virtual ~CacheGetResponse();

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Value. */
                Readable& value;
            };

            /**
             * Cache put response.
             */
            class BinaryTypeGetResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param snapshot Type snapshot.
                 */
                BinaryTypeGetResponse(binary::SPSnap& snapshot) :
                    snapshot(snapshot)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~BinaryTypeGetResponse()
                {
                    // No-op.
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Cache ID. */
                binary::SPSnap& snapshot;
            };

            /**
             * Get cache names response.
             */
            class GetCacheNamesResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheNames Cache names.
                 */
                GetCacheNamesResponse(std::vector<std::string>& cacheNames) :
                    cacheNames(cacheNames)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~GetCacheNamesResponse()
                {
                    // No-op.
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Cache ID. */
                std::vector<std::string>& cacheNames;
            };

            /**
             * Get cache names response.
             */
            class BoolResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 */
                BoolResponse() :
                    value(false)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~BoolResponse()
                {
                    // No-op.
                }

                /**
                 * Get received value.
                 *
                 * @return Received bool value.
                 */
                bool GetValue() const
                {
                    return value;
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Value. */
                bool value;
            };

            /**
             * Get cache names response.
             */
            class Int64Response : public Response
            {
            public:
                /**
                 * Constructor.
                 */
                Int64Response() :
                    value(0)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~Int64Response()
                {
                    // No-op.
                }

                /**
                 * Get received value.
                 *
                 * @return Received bool value.
                 */
                int64_t GetValue() const
                {
                    return value;
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Value. */
                int64_t value;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_MESSAGE
