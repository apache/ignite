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
 * Declares ignite::cache::query::QueryArgument class template and
 * ignite::cache::query::QueryArgumentBase interface.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_ARGUMENT
#define _IGNITE_CACHE_QUERY_QUERY_ARGUMENT

#include "ignite/binary/binary_raw_writer.h"

namespace ignite
{
    namespace cache
    {
        namespace query
        {
            /**
             * Base class for all query arguments.
             */
            class QueryArgumentBase
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~QueryArgumentBase()
                {
                    // No-op.
                }

                /**
                 * Copy argument. 
                 *
                 * @return Copy of this argument instance.
                 */
                virtual QueryArgumentBase* Copy() const = 0;

                /**
                 * Write argument using provided writer.
                 *
                 * @param writer Writer to use to write this argument.
                 */
                virtual void Write(ignite::binary::BinaryRawWriter& writer) = 0;
            };

            /**
             * Query argument class template.
             *
             * Template argument type should be copy-constructable and
             * assignable. Also BinaryType class template should be specialized
             * for this type.
             */
            template<typename T>
            class QueryArgument : public QueryArgumentBase
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param val Value.
                 */
                QueryArgument(const T& val) :
                    val(val)
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                QueryArgument(const QueryArgument& other) :
                    val(other.val)
                {
                    // No-op.
                }

                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 * @return *this.
                 */
                QueryArgument& operator=(const QueryArgument& other) 
                {
                    if (this != &other)
                        val = other.val;

                    return *this;
                }

                virtual ~QueryArgument()
                {
                    // No-op.
                }

                virtual QueryArgumentBase* Copy() const
                {
                    return new QueryArgument(val);
                }

                virtual void Write(ignite::binary::BinaryRawWriter& writer)
                {
                    writer.WriteObject<T>(val);
                }

            private:
                /** Value. */
                T val; 
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_ARGUMENT