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

#include "ignite/cache/query/query_argument.h"
#include "ignite/binary/binary_raw_writer.h"

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
                    pageSize(1024),
                    loc(false),
                    distributedJoins(false),
                    enforceJoinOrder(false),
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
                    pageSize(1024),
                    loc(false),
                    distributedJoins(false),
                    enforceJoinOrder(false),
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
                    pageSize(other.pageSize),
                    loc(other.loc),
                    distributedJoins(other.distributedJoins),
                    enforceJoinOrder(other.enforceJoinOrder),
                    args()
                {
                    args.reserve(other.args.size());

                    typedef std::vector<QueryArgumentBase*>::const_iterator Iter;

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
                    typedef std::vector<QueryArgumentBase*>::const_iterator Iter;

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
                        std::swap(sql, other.sql);
                        std::swap(pageSize, other.pageSize);
                        std::swap(loc, other.loc);
                        std::swap(distributedJoins, other.distributedJoins);
                        std::swap(enforceJoinOrder, other.enforceJoinOrder);
                        std::swap(args, other.args);
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
                 * If set to true query optimizer will not reorder tables in
                 * join. By default is false.
                 *
                 * It is not recommended to enable this property unless you are
                 * sure that your indexes and the query itself are correct and
                 * tuned as much as possible but query optimizer still produces
                 * wrong join order.
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
                 * Template argument type should be copy-constructable and
                 * assignable. Also BinaryType class template should be specialized
                 * for this type.
                 *
                 * @param arg Argument.
                 */
                template<typename T>
                void AddArgument(const T& arg)
                {
                    args.push_back(new QueryArgument<T>(arg));
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

                    for (std::vector<QueryArgumentBase*>::const_iterator it = args.begin(); it != args.end(); ++it)
                        (*it)->Write(writer);

                    writer.WriteBool(distributedJoins);
                    writer.WriteBool(enforceJoinOrder);
                }

            private:
                /** SQL string. */
                std::string sql;

                /** Page size. */
                int32_t pageSize;

                /** Local flag. */
                bool loc;

                /** Distributed joins flag. */
                bool distributedJoins;

                /** Enforce join order flag. */
                bool enforceJoinOrder;

                /** Arguments. */
                std::vector<QueryArgumentBase*> args;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_SQL_FIELDS