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

#ifndef _IGNITE_IMPL_BINARY_BINARY_ID_RESOLVER
#define _IGNITE_IMPL_BINARY_BINARY_ID_RESOLVER

#include "ignite/binary/binary_type.h"

namespace ignite
{
    namespace impl
    {
        namespace binary
        {
            /**
             * Binary type id resolver.
             */
            class BinaryIdResolver
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~BinaryIdResolver()
                {
                    // No-op.
                }

                /**
                 * Get binary object type ID.
                 *
                 * @return Type ID.
                 */
                virtual int32_t GetTypeId() = 0;

                /**
                 * Get binary object field ID.
                 *
                 * @param typeId Type ID.
                 * @param name Field name.
                 * @return Field ID.
                 */
                virtual int32_t GetFieldId(const int32_t typeId, const char* name) = 0;
            };

            /**
             * Templated binary type descriptor.
             */
            template<typename T>
            class TemplatedBinaryIdResolver : public BinaryIdResolver
            {
            public:
                /**
                 * Constructor.
                 */
                TemplatedBinaryIdResolver()
                {
                    type = ignite::binary::BinaryType<T>();
                }

                /**
                 * Constructor.
                 *
                 * @param type Binary type.
                 */
                TemplatedBinaryIdResolver(ignite::binary::BinaryType<T> type) : type(type)
                {
                    // No-op.
                }

                virtual int32_t GetTypeId()
                {
                    return type.GetTypeId();
                }

                virtual int32_t GetFieldId(const int32_t typeId, const char* name) {
                    if (name)
                        return type.GetFieldId(name);
                    else
                        IGNITE_ERROR_1(IgniteError::IGNITE_ERR_BINARY, "Field name cannot be NULL.");
                }
            private:
                /** Actual type.  */
                ignite::binary::BinaryType<T> type; 
            };
        }
    }
}

#endif //_IGNITE_IMPL_BINARY_BINARY_ID_RESOLVER
