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

#ifndef _IGNITE_IMPL_THIN_REMOTE_TYPE_UPDATER
#define _IGNITE_IMPL_THIN_REMOTE_TYPE_UPDATER

#include <ignite/ignite_error.h>
#include <ignite/impl/binary/binary_type_snapshot.h>
#include <ignite/impl/binary/binary_type_updater.h>

namespace ignite
{    
    namespace impl
    {
        namespace thin
        {
            /* Forward declaration. */
            class DataRouter;

            namespace net
            {
                /**
                 * Remote type updater.
                 */
                class RemoteTypeUpdater : public binary::BinaryTypeUpdater
                {
                public:
                    /**
                     * Constructor.
                     *
                     * @param router Data router.
                     */
                    RemoteTypeUpdater(DataRouter& router);

                    /**
                     * Destructor.
                     */
                    virtual ~RemoteTypeUpdater();

                    /**
                     * Update type using provided snapshot.
                     *
                     * @param snapshot Snapshot.
                     * @param err Error.
                     * @return True on success.
                     */
                    virtual bool Update(const binary::Snap& snapshot, IgniteError& err);

                    /**
                     * Get schema for type.
                     *
                     * @param typeId Type ID.
                     * @param err Error.
                     * @return Result.
                     */
                    virtual binary::SPSnap GetMeta(int32_t typeId, IgniteError& err);

                private:
                    /** Data router. */
                    DataRouter& router;
                };
            }
        }
    }    
}

#endif //_IGNITE_IMPL_THIN_REMOTE_TYPE_UPDATER