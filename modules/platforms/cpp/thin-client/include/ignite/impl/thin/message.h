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

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include <ignite/impl/thin/protocol_version.h>
#include <ignite/thin/ignite_client_configuration.h>

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

                    /** Cache get names. */
                    CACHE_GET_NAMES = 1050,

                    /** Cache create with name. */
                    CACHE_CREATE_WITH_NAME = 1051,

                    /** Cache get or create with name. */
                    CACHE_GET_OR_CREATE_WITH_NAME = 1052,

                    /** Cache destroy. */
                    CACHE_DESTROY = 1056,

                    /** Cache get. */
                    CACHE_GET = 1000,

                    /** Cache put. */
                    CACHE_PUT = 1001,

                    /** Cache contains key. */
                    CACHE_CONTAINS_KEY = 1011,

                    /** Cache contains keys. */
                    CACHE_CONTAINS_KEYS = 1012,

                    /** Get size. */
                    CACHE_GET_SIZE = 1020,

                    /** Get all. */
                    CACHE_GET_ALL = 1003,

                    /** Put all. */
                    CACHE_PUT_ALL = 1004,

                    /** Cache replace. */
                    CACHE_REPLACE = 1009,

                    /** Cache remove all. */
                    CACHE_REMOVE_ALL = 1019,

                    /** Cache remove key. */
                    CACHE_REMOVE_KEY = 1016,

                    /** Cache remove keys. */
                    CACHE_REMOVE_KEYS = 1018,

                    /** Cache clear. */
                    CACHE_CLEAR = 1013,

                    /** Cache clear key. */
                    CACHE_CLEAR_KEY = 1014,

                    /** Cache clear keys. */
                    CACHE_CLEAR_KEYS = 1015,

                    /** Cache replace if equals. */
                    CACHE_REPLACE_IF_EQUALS = 1010,

                    /** Cache remove if equals. */
                    CACHE_REMOVE_IF_EQUALS = 1017,

                    /** Cache get and put. */
                    CACHE_GET_AND_PUT = 1005,

                    /** Cache get and remove. */
                    CACHE_GET_AND_REMOVE = 1007,

                    /** Cache get and replace. */
                    CACHE_GET_AND_REPLACE = 1006,

                    /** Cache put if absent. */
                    CACHE_PUT_IF_ABSENT = 1002,

                    /** Cache get and put if absent. */
                    CACHE_GET_AND_PUT_IF_ABSENT = 1008,

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
                 * Get operation code.
                 *
                 * @return Operation code.
                 */
                static int32_t GetOperationCode()
                {
                    return OpCode;
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
                ~GetOrCreateCacheWithNameRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

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
                ~CreateCacheWithNameRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

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
                ~DestroyCacheRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Cache ID. */
                int32_t cacheId;
            };

            /**
             * Cache put request.
             */
            class CachePutRequest : public Request<RequestType::CACHE_PUT>
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
                ~CachePutRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Cache ID. */
                int32_t cacheId;

                /** Binary flag. */
                bool binary;

                /** Key. */
                const Writable& key;

                /** Value. */
                const Writable& value;
            };

            /**
             * Cache put request.
             */
            class CacheGetRequest : public Request<RequestType::CACHE_GET>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param key Key.
                 */
                CacheGetRequest(int32_t cacheId, bool binary, const Writable& key);

                /**
                 * Destructor.
                 */
                ~CacheGetRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

            private:
                /** Cache ID. */
                int32_t cacheId;

                /** Binary flag. */
                bool binary;

                /** Key. */
                const Writable& key;
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
                ~BinaryTypeGetRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

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
                ~BinaryTypePutRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion& ver) const;

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
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Value. */
                Readable& value;
            };

            /**
             * Cache put request.
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
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Cache ID. */
                binary::SPSnap& snapshot;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_MESSAGE
