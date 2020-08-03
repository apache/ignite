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
 * Declares ignite::cache::query::SqlQuery class.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_SQL
#define _IGNITE_CACHE_QUERY_QUERY_SQL

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
             * Sql query.
             *
             * @deprecated Will be removed in future releases. Use SqlFieldsQuery instead.
             */
            class SqlQuery
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param type Type name.
                 * @param sql SQL string.
                 */
                SqlQuery(const std::string& type, const std::string& sql) :
                    type(type),
                    sql(sql),
                    pageSize(1024),
                    loc(false),
                    distributedJoins(false),
                    args()
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                SqlQuery(const SqlQuery& other) :
                    type(other.type),
                    sql(other.sql),
                    pageSize(other.pageSize),
                    loc(other.loc),
                    distributedJoins(other.distributedJoins),
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
                SqlQuery& operator=(const SqlQuery& other) 
                {
                    if (this != &other)
                    {
                        SqlQuery tmp(other);

                        Swap(tmp);
                    }

                    return *this;
                }

                /**
                 * Destructor.
                 */
                ~SqlQuery()
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
                void Swap(SqlQuery& other)
                {
                    if (this != &other)
                    {
                        std::swap(type, other.type);
                        std::swap(sql, other.sql);
                        std::swap(pageSize, other.pageSize);
                        std::swap(loc, other.loc);
                        std::swap(distributedJoins, other.distributedJoins);
                        std::swap(args, other.args);
                    }
                }

                /**
                 * Get type name.
                 *
                 * @return Type name.
                 */
                const std::string& GetType() const
                {
                    return type;
                }

                /**
                 * Set type name.
                 *
                 * @param sql Type name.
                 */
                void SetType(const std::string& type)
                {
                    this->type = type;
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
                    args.push_back(new impl::cache::query::QueryArgument<T>(arg));
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
                 * Write query info to the stream.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryRawWriter& writer) const
                {
                    writer.WriteBool(loc);
                    writer.WriteString(sql);
                    writer.WriteString(type);
                    writer.WriteInt32(pageSize);

                    writer.WriteInt32(static_cast<int32_t>(args.size()));

                    std::vector<impl::cache::query::QueryArgumentBase*>::const_iterator it;

                    for (it = args.begin(); it != args.end(); ++it)
                        (*it)->Write(writer);

                    writer.WriteBool(distributedJoins);
                    writer.WriteInt32(0);  // Timeout, ms
                    writer.WriteBool(false);  // ReplicatedOnly
                }

            private:
                /** Type name. */
                std::string type;

                /** SQL string. */
                std::string sql;

                /** Page size. */
                int32_t pageSize;

                /** Local flag. */
                bool loc;

                /** Distributed joins flag. */
                bool distributedJoins;

                /** Arguments. */
                std::vector<impl::cache::query::QueryArgumentBase*> args;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_SQL
