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

/**
 * @file
 * Declares ignite::cache::query::SqlFieldsQuery class.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_SQL_FIELDS
#define _IGNITE_CACHE_QUERY_QUERY_SQL_FIELDS

#include <stdint.h>
#include <string>
#include <vector>

#include <ignite/impl/cache/query/query_argument.h>
#include <ignite/binary/binary_raw_writer.h>

namespace ignite
{
    namespace cache
    {
        namespace query
        {
            /**
             * Sql fields query.
             */
            class SqlFieldsQuery
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param sql SQL string.
                 */
                SqlFieldsQuery(const std::string& sql) :
                    sql(sql),
                    schema(),
                    pageSize(1024),
                    loc(false),
                    distributedJoins(false),
                    enforceJoinOrder(false),
                    lazy(false),
                    args()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param sql SQL string.
                 * @param loc Whether query should be executed locally.
                 */
                SqlFieldsQuery(const std::string& sql, bool loc) :
                    sql(sql),
                    schema(),
                    pageSize(1024),
                    loc(loc),
                    distributedJoins(false),
                    enforceJoinOrder(false),
                    lazy(false),
                    args()
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                SqlFieldsQuery(const SqlFieldsQuery& other) :
                    sql(other.sql),
                    schema(other.schema),
                    pageSize(other.pageSize),
                    loc(other.loc),
                    distributedJoins(other.distributedJoins),
                    enforceJoinOrder(other.enforceJoinOrder),
                    lazy(other.lazy),
                    args()
                {
                    args.reserve(other.args.size());

                    typedef std::vector<impl::cache::query::QueryArgumentBase*>::const_iterator Iter;

                    for (Iter i = other.args.begin(); i != other.args.end(); ++i)
                        args.push_back((*i)->Copy());
                }

                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 */
                SqlFieldsQuery& operator=(const SqlFieldsQuery& other) 
                {
                    if (this != &other)
                    {
                        SqlFieldsQuery tmp(other);

                        Swap(tmp);
                    }

                    return *this;
                }

                /**
                 * Destructor.
                 */
                ~SqlFieldsQuery()
                {
                    typedef std::vector<impl::cache::query::QueryArgumentBase*>::const_iterator Iter;

                    for (Iter it = args.begin(); it != args.end(); ++it)
                        delete *it;
                }

                /**
                 * Efficiently swaps contents with another SqlQuery instance.
                 *
                 * @param other Other instance.
                 */
                void Swap(SqlFieldsQuery& other)
                {
                    if (this != &other)
                    {
                        using std::swap;

                        swap(sql, other.sql);
                        swap(schema, other.schema);
                        swap(pageSize, other.pageSize);
                        swap(loc, other.loc);
                        swap(distributedJoins, other.distributedJoins);
                        swap(enforceJoinOrder, other.enforceJoinOrder);
                        swap(lazy, other.lazy);
                        swap(args, other.args);
                    }
                }

                /**
                 * Get SQL string.
                 *
                 * @return SQL string.
                 */
                const std::string& GetSql() const
                {
                    return sql;
                }

                /**
                 * Set SQL string.
                 *
                 * @param sql SQL string.
                 */
                void SetSql(const std::string& sql)
                {
                    this->sql = sql;
                }

                /**
                 * Get page size.
                 *
                 * @return Page size.
                 */
                int32_t GetPageSize() const
                {
                    return pageSize;
                }

                /**
                 * Set page size.
                 *
                 * @param pageSize Page size.
                 */
                void SetPageSize(int32_t pageSize)
                {
                    this->pageSize = pageSize;
                }

                /**
                 * Get local flag.
                 *
                 * @return Local flag.
                 */
                bool IsLocal() const
                {
                    return loc;
                }

                /**
                 * Set local flag.
                 *
                 * @param loc Local flag.
                 */
                void SetLocal(bool loc)
                {
                    this->loc = loc;
                }

                /**
                 * Gets lazy query execution flag.
                 *
                 * See SetLazy(bool) for more information.
                 *
                 * @return Lazy flag.
                 */
                bool IsLazy() const
                {
                    return lazy;
                }

                /**
                 * Sets lazy query execution flag.
                 *
                 * By default Ignite attempts to fetch the whole query result set to memory and send it to the client.
                 * For small and medium result sets this provides optimal performance and minimize duration of internal
                 * database locks, thus increasing concurrency.
                 *
                 * If result set is too big to fit in available memory this could lead to excessive GC pauses and even
                 * OutOfMemoryError. Use this flag as a hint for Ignite to fetch result set lazily, thus minimizing
                 * memory consumption at the cost of moderate performance hit.
                 *
                 * Defaults to @c false, meaning that the whole result set is fetched to memory eagerly.
                 *
                 * @param lazy Lazy query execution flag.
                 */
                void SetLazy(bool lazy)
                {
                    this->lazy = lazy;
                }

                /**
                 * Checks if join order of tables if enforced.
                 *
                 * @return Flag value.
                 */
                bool IsEnforceJoinOrder() const
                {
                    return enforceJoinOrder;
                }

                /**
                 * Sets flag to enforce join order of tables in the query.
                 *
                 * If set to true query optimizer will not reorder tables in join. By default is false.
                 *
                 * It is not recommended to enable this property unless you are sure that your indexes and the query
                 * itself are correct and tuned as much as possible but query optimizer still produces wrong join order.
                 *
                 * @param enforce Flag value.
                 */
                void SetEnforceJoinOrder(bool enforce)
                {
                    enforceJoinOrder = enforce;
                }

                /**
                 * Check if distributed joins are enabled for this query.
                 *
                 * @return True If distributed joind enabled.
                 */
                bool IsDistributedJoins() const
                {
                    return distributedJoins;
                }

                /**
                 * Specify if distributed joins are enabled for this query.
                 *
                 * When disabled, join results will only contain colocated data (joins work locally).
                 * When enabled, joins work as expected, no matter how the data is distributed.
                 *
                 * @param enabled Distributed joins enabled.
                 */
                void SetDistributedJoins(bool enabled)
                {
                    distributedJoins = enabled;
                }

                /**
                 * Add argument.
                 *
                 * Template argument type should be copy-constructable and assignable. Also BinaryType class template
                 * should be specialized for this type.
                 *
                 * @param arg Argument.
                 */
                template<typename T>
                void AddArgument(const T& arg)
                {
                    args.push_back(new impl::cache::query::QueryArgument<T>(arg));
                }

                /**
                 * Add array of bytes as an argument.
                 *
                 * @param src Array pointer.
                 * @param len Array length in bytes.
                 */
                void AddInt8ArrayArgument(const int8_t* src, int32_t len)
                {
                    args.push_back(new impl::cache::query::QueryInt8ArrayArgument(src, len));
                }

                /**
                 * Remove all added arguments.
                 */
                void ClearArguments()
                {
                    std::vector<impl::cache::query::QueryArgumentBase*>::iterator iter;
                    for (iter = args.begin(); iter != args.end(); ++iter)
                        delete *iter;

                    args.clear();
                }

                /**
                 * Set schema name for the query.
                 * If not set, current cache name is used, which means you can omit schema name for tables within the
                 * current cache.
                 *
                 * @param schema Schema. Empty string to unset.
                 */
                void SetSchema(const std::string& schema)
                {
                    this->schema = schema;
                }

                /**
                 * Get schema name for the query.
                 *
                 * If not set, current cache name is used, which means you can omit schema name for tables within the
                 * current cache.
                 *
                 * @return Schema. Empty string if not set.
                 */
                const std::string& GetSchema() const
                {
                    return schema;
                }

                /**
                 * Write query info to the stream.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryRawWriter& writer) const
                {
                    writer.WriteBool(loc);
                    writer.WriteString(sql);
                    writer.WriteInt32(pageSize);

                    writer.WriteInt32(static_cast<int32_t>(args.size()));

                    std::vector<impl::cache::query::QueryArgumentBase*>::const_iterator it;

                    for (it = args.begin(); it != args.end(); ++it)
                        (*it)->Write(writer);

                    writer.WriteBool(distributedJoins);
                    writer.WriteBool(enforceJoinOrder);
                    writer.WriteBool(lazy);
                    writer.WriteInt32(0);       // Timeout, ms
                    writer.WriteBool(false);    // ReplicatedOnly
                    writer.WriteBool(false);    // Colocated

                    if (schema.empty())
                        writer.WriteNull();
                    else
                        writer.WriteString(schema);

                    writer.WriteInt32Array(NULL, 0); // Partitions
                    writer.WriteInt32(1);                // UpdateBatchSize
                }

            private:
                /** SQL string. */
                std::string sql;

                /** SQL Schema. */
                std::string schema;

                /** Page size. */
                int32_t pageSize;

                /** Local flag. */
                bool loc;

                /** Distributed joins flag. */
                bool distributedJoins;

                /** Enforce join order flag. */
                bool enforceJoinOrder;

                /** Lazy flag. */
                bool lazy;

                /** Arguments. */
                std::vector<impl::cache::query::QueryArgumentBase*> args;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_SQL_FIELDS
