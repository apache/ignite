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

#ifndef _IGNITE_CACHE_QUERY_SQL
#define _IGNITE_CACHE_QUERY_SQL

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
             * Sql query.
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
                SqlQuery(const std::string& type, const std::string& sql) : type(type), sql(sql), pageSize(1024), 
                    loc(false), args(NULL)
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                SqlQuery(const SqlQuery& other) : type(other.type), sql(other.sql), pageSize(other.pageSize),
                    loc(other.loc), args()
                {
                    args.reserve(other.args.size());

                    for (std::vector<QueryArgumentBase*>::const_iterator i = other.args.begin(); 
                        i != other.args.end(); ++i)
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

                        std::swap(type, tmp.type);
                        std::swap(sql, tmp.sql);
                        std::swap(pageSize, tmp.pageSize);
                        std::swap(loc, tmp.loc);
                        std::swap(args, tmp.args);
                    }

                    return *this;
                }

                /**
                 * Destructor.
                 */
                ~SqlQuery()
                {
                    for (std::vector<QueryArgumentBase*>::iterator it = args.begin(); it != args.end(); ++it)
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
                 * Add argument.
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
                    writer.WriteString(type);
                    writer.WriteInt32(pageSize);

                    writer.WriteInt32(static_cast<int32_t>(args.size()));

                    for (std::vector<QueryArgumentBase*>::const_iterator it = args.begin(); it != args.end(); ++it)
                        (*it)->Write(writer);
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

                /** Arguments. */
                std::vector<QueryArgumentBase*> args;
            };
        }
    }    
}

#endif