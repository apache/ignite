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

#ifndef _IGNITE_IMPL_BINARY_BINARY_TYPE_UPDATER
#define _IGNITE_IMPL_BINARY_BINARY_TYPE_UPDATER

#include <ignite/ignite_error.h>
#include <ignite/impl/binary/binary_type_snapshot.h>

namespace ignite
{    
    namespace impl
    {
        namespace binary
        {
            /**
             * Type updater interface.
             */
            class IGNITE_IMPORT_EXPORT BinaryTypeUpdater
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~BinaryTypeUpdater()
                {
                    // No-op.
                }

                /**
                 * Update type using provided snapshot.
                 *
                 * @param snapshot Snapshot.
                 * @param err Error.
                 * @return True on success.
                 */
                virtual bool Update(const Snap& snapshot, IgniteError& err) = 0;

                /**
                 * Get schema for type.
                 *
                 * @param typeId Type ID.
                 * @param err Error.
                 * @return Result.
                 */
                virtual SPSnap GetMeta(int32_t typeId, IgniteError& err) = 0;
            };
        }
    }    
}

#endif //_IGNITE_IMPL_BINARY_BINARY_TYPE_UPDATER
