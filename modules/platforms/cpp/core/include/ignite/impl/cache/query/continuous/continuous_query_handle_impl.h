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

/**
 * @file
 * Declares ignite::impl::cache::query::continuous::ContinuousQueryHandleImpl class.
 */

#ifndef _IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL
#define _IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL

#include "ignite/cache/query/query_cursor.h"
#include "ignite/impl/cache/query/continuous/continuous_query_impl.h"

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                namespace continuous
                {
                    /**
                     * Continuous query handle implementation.
                     */
                    class IGNITE_IMPORT_EXPORT ContinuousQueryHandleImpl
                    {
                        typedef common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
                        typedef common::concurrent::SharedPointer<ContinuousQueryImplBase> SP_ContinuousQueryImplBase;
                    public:
                        /**
                         * Default constructor.
                         * 
                         * @param env Environment.
                         * @param javaRef Java reference.
                         */
                        ContinuousQueryHandleImpl(SP_IgniteEnvironment env, int64_t handle, jobject javaRef);

                        /**
                         * Destructor.
                         */
                        ~ContinuousQueryHandleImpl();

                        /**
                         * Gets the cursor for initial query.
                         * Can be called only once, throws exception on consequent calls.
                         *
                         * @param err Error.
                         * @return Initial query cursor.
                         */
                        QueryCursorImpl* GetInitialQueryCursor(IgniteError& err);

                    private:
                        /** Environment. */
                        SP_IgniteEnvironment env;

                        /** Local handle for handle registry. */
                        int64_t handle;

                        /** Handle to Java object. */
                        jobject javaRef;

                        /** Mutex. */
                        common::concurrent::CriticalSection mutex;

                        /** Cursor extracted. */
                        bool extracted;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_CONTINUOUS_CONTINUOUS_QUERY_HANDLE_IMPL