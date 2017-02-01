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
 * Declares ignite::binary::BinaryIdentityResolver class.
 */

#ifndef _IGNITE_BINARY_BINARY_IDENTITY_RESOLVER
#define _IGNITE_BINARY_BINARY_IDENTITY_RESOLVER

#include <stdint.h>

#include <ignite/common/common.h>

#include <ignite/binary/binary_object.h>

namespace ignite
{
    namespace binary
    {
        /**
         * Binary identity resolver.
         */
        class IGNITE_IMPORT_EXPORT BinaryIdentityResolver
        {
        public:
            /**
             * Destructor.
             */
            virtual ~BinaryIdentityResolver()
            {
                // No-op.
            }

            /**
             * Get binary object hash code.
             *
             * @param obj Binary object.
             * @return Hash code.
             */
            virtual int32_t GetHashCode(const BinaryObject& obj) = 0;
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_IDENTITY_RESOLVER
