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
 * Declares ignite::cache::query::QueryFieldsCursor class.
 */

#ifndef _IGNITE_CACHE_QUERY_FIELDS_CURSOR
#define _IGNITE_CACHE_QUERY_FIELDS_CURSOR

#include <vector>

#include <ignite/common/concurrent.h>

#include "ignite/cache/cache_entry.h"
#include "ignite/ignite_error.h"
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
             */
            class QueryFieldsCursor
            {
            public:
                /**
                 * Default constructor.
                 */
                QueryFieldsCursor() : impl(NULL)
                {
                    // No-op.
                }

                /**
                 * Constructor.
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
                 * @return True if next entry exists.
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
                 *
                 * @param err Error.
                 * @return True if next entry exists.
                 */
                bool HasNext(IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->HasNext(&err);
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
                 * @return Next entry.
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
                 *
                 * @param err Error.
                 * @return Next entry.
                 */
                QueryFieldsRow GetNext(IgniteError& err)
                {
                    impl::cache::query::QueryCursorImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->GetNextRow(&err);
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
                 * @return True if the instance is valid and can be used.
                 */
                bool IsValid()
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

#endif