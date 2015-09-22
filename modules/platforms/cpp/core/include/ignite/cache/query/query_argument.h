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

#ifndef _IGNITE_CACHE_QUERY_ARGUMENT
#define _IGNITE_CACHE_QUERY_ARGUMENT

#include "ignite/portable/portable_raw_writer.h"

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
                 * @return Copy.
                 */
                virtual QueryArgumentBase* Copy() = 0;

                /**
                 * Write argument.
                 */
                virtual void Write(ignite::portable::PortableRawWriter& writer) = 0;
            };

            /**
             * Query argument.
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
                QueryArgument(const T& val) : val(val)
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Other instance.
                 */
                QueryArgument(const QueryArgument& other)
                {
                    val = other.val;
                }

                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 */
                QueryArgument& operator=(const QueryArgument& other) 
                {
                    if (this != &other)
                    {
                        QueryArgument tmp(other);

                        T val0 = val;
                        val = tmp.val;
                        tmp.val = val0;
                    }

                    return *this;
                }

                ~QueryArgument()
                {
                    // No-op.
                }

                QueryArgumentBase* Copy()
                {
                    return new QueryArgument(val);
                }

                void Write(ignite::portable::PortableRawWriter& writer)
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

#endif