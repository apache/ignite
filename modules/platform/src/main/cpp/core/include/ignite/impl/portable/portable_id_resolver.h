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

#ifndef _IGNITE_IMPL_PORTABLE_ID_RESOLVER
#define _IGNITE_IMPL_PORTABLE_ID_RESOLVER

#include "ignite/portable/portable_type.h"

namespace ignite
{
    namespace impl
    {
        namespace portable
        {
            /**
             * Portable type id resolver.
             */
            class PortableIdResolver
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~PortableIdResolver()
                {
                    // No-op.
                }

                /**
                 * Get portable object type ID.
                 *
                 * @return Type ID.
                 */
                virtual int32_t GetTypeId() = 0;

                /**
                 * Get portable object field ID.
                 *
                 * @param typeId Type ID.
                 * @param name Field name.
                 * @return Field ID.
                 */
                virtual int32_t GetFieldId(const int32_t typeId, const char* name) = 0;
            };

            /**
             * Templated portable type descriptor.
             */
            template<typename T>
            class TemplatedPortableIdResolver : public PortableIdResolver
            {
            public:
                /**
                 * Constructor.
                 */
                TemplatedPortableIdResolver()
                {
                    type = ignite::portable::PortableType<T>();
                }

                /**
                 * Constructor.
                 *
                 * @param type Portable type.
                 */
                TemplatedPortableIdResolver(ignite::portable::PortableType<T> type) : type(type)
                {
                    // No-op.
                }

                virtual int32_t GetTypeId()
                {
                    return type.GetTypeId();
                }

                virtual int32_t GetFieldId(const int32_t typeId, const char* name) {
                    if (!name)
                    {
                        IGNITE_ERROR_1(IgniteError::IGNITE_ERR_PORTABLE, "Field name cannot be NULL.");
                    }

                    return type.GetFieldId(name);
                }
            private:
                /** Actual type.  */
                ignite::portable::PortableType<T> type; 
            };
        }
    }
}

#endif