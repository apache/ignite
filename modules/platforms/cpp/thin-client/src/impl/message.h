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

#include <ignite/thin/cache/query/query_sql_fields.h>
#include <ignite/thin/transactions/transaction_consts.h>

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/binary/binary_reader_impl.h>

#include <ignite/impl/thin/writable.h>
#include <ignite/impl/thin/readable.h>

#include "impl/affinity/affinity_topology_version.h"
#include "impl/affinity/partition_awareness_group.h"
#include "impl/cache/query/cursor_page.h"
#include "impl/protocol_version.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** "Transactional" flag mask. */
            #define TRANSACTIONAL_FLAG_MASK 0x02;

            #define KEEP_BINARY_FLAG_MASK 0x01;

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

                    /** Cache partitions request. */
                    CACHE_PARTITIONS = 1101,

                    /** SQL fields query request. */
                    QUERY_SQL_FIELDS = 2004,

                    /** SQL fields query get next cursor page request. */
                    QUERY_SQL_FIELDS_CURSOR_GET_PAGE = 2005,

                    /** Get binary type info. */
                    GET_BINARY_TYPE = 3002,

                    /** Put binary type info. */
                    PUT_BINARY_TYPE = 3003,

                    /** Start new transaction. */
                    OP_TX_START = 4000,

                    /** Commit transaction. */
                    OP_TX_END = 4001,

                    /** Execute compute task. */
                    COMPUTE_TASK_EXECUTE = 6000,

                    /** Compute task completion notification. */
                    COMPUTE_TASK_FINISHED = 6001,
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
                virtual void Write(binary::BinaryWriterImpl&, const ProtocolVersion&) const
                {
                    // No-op.
                }
            };

            /**
             * Cache partitions request.
             */
            class CachePartitionsRequest : public Request<RequestType::CACHE_PARTITIONS>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheIds Cache IDs.
                 */
                CachePartitionsRequest(const std::vector<int32_t>& cacheIds);

                /**
                 * Destructor.
                 */
                virtual ~CachePartitionsRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const;

            private:
                /** Cache IDs. */
                const std::vector<int32_t>& cacheIds;
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
                    binary(binary),
                    actTx(false),
                    txId(0)
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
                 * Sets transaction active flag and appropriate txId.
                 * @param active Transaction activity flag.
                 * @param id Transaction id.
                 */
                void activeTx(bool active, int32_t id) {
                    actTx = active;

                    txId = id;
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const
                {
                    writer.WriteInt32(cacheId);

                    int8_t flags = 0;

                    if (binary)
                        flags |= KEEP_BINARY_FLAG_MASK;

                    if (actTx)
                        flags |= TRANSACTIONAL_FLAG_MASK;

                    writer.WriteInt8(flags);

                    if (actTx)
                        writer.WriteInt32(txId);
                }

            private:
                /** Cache ID. */
                int32_t cacheId;

                /** Binary flag. */
                bool binary;

                bool actTx;

                int32_t txId;
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
             * Cache value request.
             *
             * Request to cache containing writable value.
             */
            template<int32_t OpCode>
            class CacheValueRequest : public CacheRequest<OpCode>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param value Value.
                 */
                CacheValueRequest(int32_t cacheId, bool binary, const Writable& value) :
                    CacheRequest<OpCode>(cacheId, binary),
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~CacheValueRequest()
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

                    value.Write(writer);
                }

            private:
                /** Key. */
                const Writable& value;
            };

            /**
             * Cache 2 value request.
             */
            template<int32_t OpCode>
            class Cache2ValueRequest : public CacheRequest<OpCode>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param val1 Value 1.
                 * @param val2 Value 2.
                 */
                Cache2ValueRequest(int32_t cacheId, bool binary, const Writable& val1, const Writable& val2) :
                    CacheRequest<OpCode>(cacheId, binary),
                    val1(val1),
                    val2(val2)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~Cache2ValueRequest()
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

                    val1.Write(writer);
                    val2.Write(writer);
                }

            private:
                /** Value 1. */
                const Writable& val1;

                /** Value 2. */
                const Writable& val2;
            };

            /**
             * Cache 3 value request.
             */
            template<int32_t OpCode>
            class Cache3ValueRequest : public CacheRequest<OpCode>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param binary Binary cache flag.
                 * @param val1 Value 1.
                 * @param val2 Value 2.
                 * @param val3 Value 3.
                 */
                Cache3ValueRequest(int32_t cacheId, bool binary, const Writable& val1, const Writable& val2,
                    const Writable& val3) :
                    CacheRequest<OpCode>(cacheId, binary),
                    val1(val1),
                    val2(val2),
                    val3(val3)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~Cache3ValueRequest()
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

                    val1.Write(writer);
                    val2.Write(writer);
                    val3.Write(writer);
                }

            private:
                /** Value 1. */
                const Writable& val1;

                /** Value 2. */
                const Writable& val2;

                /** Value 3. */
                const Writable& val3;
            };

            /**
             * Tx start request.
             */
            class TxStartRequest : public Request<RequestType::OP_TX_START>
            {
            public:
                /**
                 * Constructor.
                 */
                TxStartRequest(
                    ignite::thin::transactions::TransactionConcurrency::Type conc,
                    ignite::thin::transactions::TransactionIsolation::Type isolationLvl,
                    int64_t tmOut,
                    ignite::common::concurrent::SharedPointer<common::FixedSizeArray<char> > lbl
                ) :
                    concurrency(conc),
                    isolation(isolationLvl),
                    timeout(tmOut),
                    label(lbl)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~TxStartRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const {
                    writer.WriteInt8(concurrency);
                    writer.WriteInt8(isolation);
                    writer.WriteInt64(timeout);
                    label.IsValid() ? writer.WriteString(label.Get()->GetData()) : writer.WriteNull();
                }

            private:
                /** Cncurrency. */
                ignite::thin::transactions::TransactionConcurrency::Type concurrency;

                /** Isolation. */
                ignite::thin::transactions::TransactionIsolation::Type isolation;

                /** Timeout. */
                const int64_t timeout;

                /** Tx label. */
                ignite::common::concurrent::SharedPointer<common::FixedSizeArray<char> > label;
            };

            /**
             * Tx end request.
             */
            class TxEndRequest : public Request<RequestType::OP_TX_END>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param id Transaction id.
                 * @param commit Need to commit flag.
                 */
                TxEndRequest(int32_t id, bool commit) :
                    txId(id),
                    commited(commit)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~TxEndRequest()
                {
                    // No-op.
                }

                /**
                 * Write request using provided writer.
                 * @param writer Writer.
                 * @param ver Version.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer, const ProtocolVersion&) const {
                    writer.WriteInt32(txId);
                    writer.WriteBool(commited);
                }

            private:
                /** Tx id. */
                const int32_t txId;

                /** Need to commit flag. */
                const bool commited;
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
             * Cache SQL fields query request.
             */
            class SqlFieldsQueryRequest : public CacheRequest<RequestType::QUERY_SQL_FIELDS>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cacheId Cache ID.
                 * @param qry SQL query.
                 */
                explicit SqlFieldsQueryRequest(int32_t cacheId, const ignite::thin::cache::query::SqlFieldsQuery &qry);

                /**
                 * Destructor.
                 */
                virtual ~SqlFieldsQueryRequest()
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
                /** Query. */
                const ignite::thin::cache::query::SqlFieldsQuery &qry;
            };

            /**
             * Cache SQL fields cursor get page request.
             */
            class SqlFieldsCursorGetPageRequest : public Request<RequestType::QUERY_SQL_FIELDS_CURSOR_GET_PAGE>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param cursorId Cursor ID.
                 */
                explicit SqlFieldsCursorGetPageRequest(int64_t cursorId) :
                    cursorId(cursorId)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~SqlFieldsCursorGetPageRequest()
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
                /** Cursor ID. */
                const int64_t cursorId;
            };

            /**
             * Compute task execute request.
             */
            class ComputeTaskExecuteRequest : public Request<RequestType::COMPUTE_TASK_EXECUTE>
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param flags Flags.
                 * @param timeout Timeout in milliseconds.
                 * @param taskName Task name.
                 * @param arg Argument.
                 */
                ComputeTaskExecuteRequest(int8_t flags, int64_t timeout, const std::string& taskName,
                    const Writable& arg) :
                    flags(flags),
                    timeout(timeout),
                    taskName(taskName),
                    arg(arg)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskExecuteRequest()
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
                /** Flags. */
                const int8_t flags;

                /** Timeout in milliseconds. */
                const int64_t timeout;

                /** Task name. */
                const std::string& taskName;

                /** Argument. */
                const Writable& arg;
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

                /**
                 * Get affinity topology version.
                 *
                 * @return Affinity topology version, or null if it has not changed.
                 */
                const AffinityTopologyVersion* GetAffinityTopologyVersion() const
                {
                    if (!IsAffinityTopologyChanged())
                        return 0;

                    return &topologyVersion;
                }

                /**
                 * Check if affinity topology failed.
                 *
                 * @return @c true affinity topology failed.
                 */
                bool IsAffinityTopologyChanged() const;

                /**
                 * Check if operation failed.
                 *
                 * @return @c true if operation failed.
                 */
                bool IsFailure() const;

            protected:
                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl&, const ProtocolVersion&)
                {
                    // No-op.
                }

            private:
                /** Flags. */
                int16_t flags;

                /** Affinity topology version. */
                AffinityTopologyVersion topologyVersion;

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
                ClientCacheNodePartitionsResponse(std::vector<NodePartitions>& nodeParts);

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
                std::vector<NodePartitions>& nodeParts;
            };

            /**
             * Cache node list request.
             */
            class CachePartitionsResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param groups Partition awareness Groups.
                 */
                CachePartitionsResponse(std::vector<PartitionAwarenessGroup>& groups);

                /**
                 * Destructor.
                 */
                virtual ~CachePartitionsResponse();

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

                /**
                 * Get version.
                 *
                 * @return Topology version.
                 */
                const AffinityTopologyVersion& GetVersion() const
                {
                    return topologyVersion;
                }

                /**
                 * Get partition awareness groups.
                 *
                 * @return Partition awareness groups.
                 */
                const std::vector<PartitionAwarenessGroup>& GetGroups() const
                {
                    return groups;
                }

            private:
                /** Affinity topology version. */
                AffinityTopologyVersion topologyVersion;

                /** Partition awareness groups. */
                std::vector<PartitionAwarenessGroup>& groups;
            };

            /**
             * Cache value response.
             */
            class CacheValueResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                CacheValueResponse(Readable& value);

                /**
                 * Destructor.
                 */
                virtual ~CacheValueResponse();

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

            /**
             * Get cache names response.
             */
            class Int32Response : public Response
            {
            public:
                /**
                 * Constructor.
                 */
                Int32Response() :
                    value(0)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~Int32Response()
                {
                    // No-op.
                }

                /**
                 * Get received value.
                 *
                 * @return Received bool value.
                 */
                int32_t GetValue() const
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
                int32_t value;
            };

            /**
             * Cache SQL fields query response.
             */
            class SqlFieldsQueryResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 */
                SqlFieldsQueryResponse() :
                    cursorId(0),
                    cursorPage(new cache::query::CursorPage())
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~SqlFieldsQueryResponse()
                {
                    // No-op.
                }

                /**
                 * Get cursor ID.
                 *
                 * @return Cursor ID.
                 */
                int64_t GetCursorId() const
                {
                    return cursorId;
                }

                /**
                 * Get columns.
                 *
                 * @return Column names.
                 */
                const std::vector<std::string>& GetColumns() const
                {
                    return columns;
                }

                /**
                 * Get cursor page.
                 * @return Cursor page.
                 */
                cache::query::SP_CursorPage GetCursorPage() const
                {
                    return cursorPage;
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Cursor ID. */
                int64_t cursorId;

                /** Column names. */
                std::vector<std::string> columns;

                /** Cursor Page. */
                cache::query::SP_CursorPage cursorPage;
            };

            /**
             * Cache SQL fields cursor get page response.
             */
            class SqlFieldsCursorGetPageResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 */
                SqlFieldsCursorGetPageResponse() :
                    cursorPage(new cache::query::CursorPage())
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~SqlFieldsCursorGetPageResponse()
                {
                    // No-op.
                }

                /**
                 * Get cursor page.
                 * @return Cursor page.
                 */
                cache::query::SP_CursorPage GetCursorPage() const
                {
                    return cursorPage;
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Cursor Page. */
                cache::query::SP_CursorPage cursorPage;
            };

            /**
             * Compute task execute response.
             */
            class ComputeTaskExecuteResponse : public Response
            {
            public:
                /**
                 * Constructor.
                 */
                ComputeTaskExecuteResponse() :
                    taskId(0)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskExecuteResponse()
                {
                    // No-op.
                }

                /**
                 * Get Notification ID.
                 * @return Notification ID.
                 */
                int64_t GetNotificationId() const
                {
                    return taskId;
                }

                /**
                 * Read data if response status is ResponseStatus::SUCCESS.
                 *
                 * @param reader Reader.
                 */
                virtual void ReadOnSuccess(binary::BinaryReaderImpl& reader, const ProtocolVersion&);

            private:
                /** Task ID. */
                int64_t taskId;
            };

            /**
             * Compute task finished notification.
             */
            class ComputeTaskFinishedNotification
            {
            public:
                typedef ComputeTaskExecuteResponse ResponseType;

                /**
                 * Constructor.
                 */
                ComputeTaskFinishedNotification(Readable& result) :
                    status(0),
                    errorMessage(),
                    result(result)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskFinishedNotification()
                {
                    // No-op.
                }

                /**
                 * Check if the message is failure.
                 * @return @c true on failure.
                 */
                bool IsFailure() const
                {
                    return !errorMessage.empty();
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
                 * Read response using provided reader.
                 * @param reader Reader.
                 * @param ver Protocol version.
                 */
                void Read(binary::BinaryReaderImpl& reader, const ProtocolVersion& ver);

            private:
                /** Status. */
                int32_t status;

                /** Error message. */
                std::string errorMessage;

                /** Result. */
                Readable& result;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_MESSAGE
