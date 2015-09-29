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

#ifndef _IGNITE_CACHE_QUERY_SQL
#define _IGNITE_CACHE_QUERY_SQL

#include <stdint.h>
#include <string>
#include <vector>

#include "ignite/cache/query/query_argument.h"
#include "ignite/portable/portable_raw_writer.h"

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
                SqlQuery(std::string type, std::string sql) : type(type), sql(sql), pageSize(1024), 
                    loc(false), args(NULL)
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                SqlQuery(const SqlQuery& other)
                {
                    type = other.type;
                    sql = other.sql;
                    pageSize = other.pageSize;
                    loc = other.loc;

                    if (other.args)
                    {
                        args = new std::vector<QueryArgumentBase*>();

                        for (std::vector<QueryArgumentBase*>::iterator it = other.args->begin();
                            it != other.args->end(); ++it)
                            args->push_back((*it)->Copy());
                    }
                    else
                        args = NULL;
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
                        type = other.type;
                        sql = other.sql;
                        pageSize = other.pageSize;
                        loc = other.loc;

                        SqlQuery tmp(other);

                        std::vector<QueryArgumentBase*>* args0 = args;

                        args = tmp.args;

                        tmp.args = args0; 
                    }

                    return *this;
                }

                /**
                 * Destructor.
                 */
                ~SqlQuery()
                {
                    if (args) 
                    {
                        for (std::vector<QueryArgumentBase*>::iterator it = args->begin(); it != args->end(); ++it)
                            delete (*it);

                        delete args;
                    }
                }

                /**
                 * Get type name.
                 *
                 * @return Type name.
                 */
                std::string GetType()
                {
                    return type;
                }

                /**
                 * Set type name.
                 *
                 * @param sql Type name.
                 */
                void SetType(std::string type)
                {
                    this->type = type;
                }

                /**
                 * Get SQL string.
                 *
                 * @return SQL string.
                 */
                std::string GetSql()
                {
                    return sql;
                }

                /**
                 * Set SQL string.
                 *
                 * @param sql SQL string.
                 */
                void SetSql(std::string sql)
                {
                    this->sql = sql;
                }

                /**
                 * Get page size.
                 *
                 * @return Page size.
                 */
                int32_t GetPageSize()
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
                bool IsLocal()
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
                    if (!args)
                        args = new std::vector<QueryArgumentBase*>();

                    args->push_back(new QueryArgument<T>(arg));
                }

                /**
                 * Write query info to the stream.
                 *
                 * @param writer Writer.
                 */
                void Write(portable::PortableRawWriter& writer) const
                {
                    writer.WriteBool(loc);
                    writer.WriteString(sql);
                    writer.WriteString(type);
                    writer.WriteInt32(pageSize);

                    if (args)
                    {
                        writer.WriteInt32(static_cast<int32_t>(args->size()));

                        for (std::vector<QueryArgumentBase*>::iterator it = args->begin(); it != args->end(); ++it)
                            (*it)->Write(writer);
                    }
                    else
                        writer.WriteInt32(0);
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
                std::vector<QueryArgumentBase*>* args;
            };
        }
    }    
}

#endif