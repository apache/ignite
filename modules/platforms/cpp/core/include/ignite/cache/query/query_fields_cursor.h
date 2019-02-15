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
 * Declares ignite::cache::query::QueryFieldsCursor class.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_FIELDS_CURSOR
#define _IGNITE_CACHE_QUERY_QUERY_FIELDS_CURSOR

#include <vector>

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include "ignite/cache/cache_entry.h"
#include "ignite/cache/query/query_fields_row.h"
#include "ignite/impl/cache/query/query_impl.h"
#include "ignite/impl/operations.h"

namespace ignite
{
    namespace cache
    {
        namespace query
        {
            /**
             * Query fields cursor.
             *
             * This class implemented as a reference to an implementation so copying
             * of this class instance will only create another reference to the same
             * underlying object. Underlying object released automatically once all
             * the instances are destructed.
             */
            class QueryFieldsCursor
            {
            public:
                /**
                 * Default constructor.
                 *
                 * Constructed instance is not valid and thus can not be used
                 * as a cursor.
                 */
                QueryFieldsCursor() : impl(0)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * Internal method. Should not be used by user.
                 *
                 * @param impl Implementation.
                 */
                QueryFieldsCursor(impl::cache::query::QueryCursorImpl* impl) : impl(impl)
                {
                    // No-op.
                }
                
                /**
                 * Check whether next entry exists.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @return True if next entry exists.
                 *
                 * @throw IgniteError class instance in case of failure.
                 */
                bool HasNext()
                {
                    IgniteError err;

                    bool res = HasNext(err);

                    IgniteError::ThrowIfNeeded(err);

                    return res;
                }

                /**
                 * Check whether next entry exists.
                 * Properly sets error param in case of failure.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @param err Used to set operation result.
                 * @return True if next entry exists and operation resulted in
                 * success. Returns false on failure.
                 */
                bool HasNext(IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->HasNext(err);
                    else
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                            "Instance is not usable (did you check for error?).");

                        return false;
                    }
                }

                /**
                 * Get next entry.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @return Next entry.
                 *
                 * @throw IgniteError class instance in case of failure.
                 */
                QueryFieldsRow GetNext()
                {
                    IgniteError err;

                    QueryFieldsRow res = GetNext(err);

                    IgniteError::ThrowIfNeeded(err);

                    return res;
                }

                /**
                 * Get next entry.
                 * Properly sets error param in case of failure.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @param err Used to set operation result.
                 * @return Next entry on success and invalid row instance on
                 * failure.
                 */
                QueryFieldsRow GetNext(IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->GetNextRow(err);
                    else
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Instance is not usable (did you check for error?).");

                        return QueryFieldsRow();
                    }
                }

                /**
                 * Check if the instance is valid.
                 *
                 * Invalid instance can be returned if some of the previous
                 * operations have resulted in a failure. For example invalid
                 * instance can be returned by not-throwing version of method
                 * in case of error. Invalid instances also often can be
                 * created using default constructor.
                 *
                 * @return True if the instance is valid and can be used.
                 */
                bool IsValid() const
                {
                    return impl.IsValid();
                }

            private:
                /** Implementation delegate. */
                ignite::common::concurrent::SharedPointer<impl::cache::query::QueryCursorImpl> impl;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_FIELDS_CURSOR
