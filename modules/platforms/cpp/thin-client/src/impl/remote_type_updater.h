/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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