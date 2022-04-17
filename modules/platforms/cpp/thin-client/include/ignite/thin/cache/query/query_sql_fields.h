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
 * Declares ignite::thin::cache::query::SqlFieldsQuery class.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_QUERY_SQL_FIELDS
#define _IGNITE_THIN_CACHE_QUERY_QUERY_SQL_FIELDS

#include <stdint.h>
#include <string>
#include <vector>

#include <ignite/impl/thin/copyable_writable.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            // Forward declaration
            class SqlFieldsQueryRequest;
        }
    }

    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                /**
                 * SQL fields query for thin client.
                 */
                class SqlFieldsQuery
                {
                public:
                    friend class ignite::impl::thin::SqlFieldsQueryRequest;

                    /**
                     * Constructor.
                     *
                     * @param sql SQL string.
                     */
                    explicit SqlFieldsQuery(const std::string& sql) :
                        sql(sql),
                        schema(),
                        pageSize(1024),
                        maxRows(0),
                        timeout(0),
                        loc(false),
                        distributedJoins(false),
                        enforceJoinOrder(false),
                        lazy(false),
                        collocated(false),
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
                        maxRows(other.maxRows),
                        timeout(other.timeout),
                        loc(other.loc),
                        distributedJoins(other.distributedJoins),
                        enforceJoinOrder(other.enforceJoinOrder),
                        lazy(other.lazy),
                        collocated(other.collocated),
                        args()
                    {
                        args.reserve(other.args.size());

                        typedef std::vector<impl::thin::CopyableWritable*>::const_iterator Iter;

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
                        ClearArguments();
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
                            swap(maxRows, other.maxRows);
                            swap(timeout, other.timeout);
                            swap(loc, other.loc);
                            swap(distributedJoins, other.distributedJoins);
                            swap(enforceJoinOrder, other.enforceJoinOrder);
                            swap(lazy, other.lazy);
                            swap(collocated, other.collocated);
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
                     * Set schema name for the query.
                     * If not set, current cache name is used, which means you can omit schema name for tables within
                     * the current cache.
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
                     * If not set, current cache name is used, which means you can omit schema name for tables within
                     * the current cache.
                     *
                     * @return Schema. Empty string if not set.
                     */
                    const std::string& GetSchema() const
                    {
                        return schema;
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
                     * Set maximum number of rows.
                     *
                     * @param maxRows Max rows.
                     */
                    void SetMaxRows(int32_t maxRows)
                    {
                        this->maxRows = maxRows;
                    }

                    /**
                     * Get maximum number of rows.
                     *
                     * @return Max rows.
                     */
                    int32_t GetMaxRows() const
                    {
                        return maxRows;
                    }

                    /**
                     * Set query execution timeout in milliseconds.
                     *
                     * @param timeout Timeout in milliseconds.
                     */
                    void SetTimeout(int64_t timeout)
                    {
                        this->timeout = timeout;
                    }

                    /**
                     * Get query execution timeout in milliseconds.
                     *
                     * @return Timeout in milliseconds.
                     */
                    int64_t GetTimeout() const
                    {
                        return timeout;
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
                     * Check if distributed joins are enabled for this query.
                     *
                     * @return True If distributed join enabled.
                     */
                    bool IsDistributedJoins() const
                    {
                        return distributedJoins;
                    }

                    /**
                     * Specify if distributed joins are enabled for this query.
                     *
                     * When disabled, join results will only contain collocated data (joins work locally).
                     * When enabled, joins work as expected, no matter how the data is distributed.
                     *
                     * @param enabled Distributed joins enabled.
                     */
                    void SetDistributedJoins(bool enabled)
                    {
                        distributedJoins = enabled;
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
                     * itself are correct and tuned as much as possible but query optimizer still produces wrong join
                     * order.
                     *
                     * @param enforce Flag value.
                     */
                    void SetEnforceJoinOrder(bool enforce)
                    {
                        enforceJoinOrder = enforce;
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
                     * By default Ignite attempts to fetch the whole query result set to memory and send it to the
                     * client. For small and medium result sets this provides optimal performance and minimize duration
                     * of internal database locks, thus increasing concurrency.
                     *
                     * If result set is too big to fit in available memory this could lead to excessive GC pauses and
                     * even OutOfMemoryError. Use this flag as a hint for Ignite to fetch result set lazily, thus
                     * minimizing memory consumption at the cost of moderate performance hit.
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
                     * Checks if this query is collocated.
                     *
                     * @return @c true If the query is collocated.
                     */
                    bool IsCollocated()
                    {
                        return collocated;
                    }

                    /**
                     * Sets flag defining if this query is collocated.
                     *
                     * Collocation flag is used for optimization purposes of queries with GROUP BY statements.
                     * Whenever Ignite executes a distributed query, it sends sub-queries to individual cluster members.
                     * If you know in advance that the elements of your query selection are collocated together on the
                     * same node and you group by collocated key (primary or affinity key), then Ignite can make
                     * significant performance and network optimizations by grouping data on remote nodes.
                     *
                     * @param collocated Flag value.
                     */
                    void SetCollocated(bool collocated)
                    {
                        this->collocated = collocated;
                    }

                    /**
                     * Add argument for the query.
                     *
                     * @tparam T Type of argument. Should be should be copy-constructable and assignable. BinaryType
                     * class template should be specialized for this type.
                     *
                     * @param arg Argument.
                     */
                    template<typename T>
                    void AddArgument(const T& arg)
                    {
                        args.push_back(new impl::thin::CopyableWritableImpl<T>(arg));
                    }

                    /**
                     * Add int8_t array as an argument.
                     *
                     * @tparam Iter Iterator type. Should provide standard iterator functionality.
                     *
                     * @param begin Begin iterator of sequence to write.
                     * @param end End iterator of sequence to write.
                     */
                    template<typename Iter>
                    void AddInt8ArrayArgument(Iter begin, Iter end)
                    {
                        args.push_back(new impl::thin::CopyableWritableInt8ArrayImpl<Iter>(begin, end));
                    }

                    /**
                     * Remove all added arguments.
                     */
                    void ClearArguments()
                    {
                        std::vector<impl::thin::CopyableWritable*>::iterator iter;
                        for (iter = args.begin(); iter != args.end(); ++iter)
                            delete *iter;

                        args.clear();
                    }

                private:
                    /** SQL string. */
                    std::string sql;

                    /** SQL Schema. */
                    std::string schema;

                    /** Page size. */
                    int32_t pageSize;

                    /** Max rows to fetch. */
                    int32_t maxRows;

                    /** Timeout. */
                    int64_t timeout;

                    /** Local flag. */
                    bool loc;

                    /** Distributed joins flag. */
                    bool distributedJoins;

                    /** Enforce join order flag. */
                    bool enforceJoinOrder;

                    /** Lazy flag. */
                    bool lazy;

                    /** Collocated flag. */
                    bool collocated;

                    /** Arguments. */
                    std::vector<impl::thin::CopyableWritable*> args;
                };
            }
        }
    }
}

#endif //_IGNITE_THIN_CACHE_QUERY_QUERY_SQL_FIELDS
