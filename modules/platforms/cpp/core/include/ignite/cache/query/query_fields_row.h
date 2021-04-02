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
 * Declares ignite::cache::query::QueryFieldsRow class.
 */

#ifndef _IGNITE_CACHE_QUERY_QUERY_FIELDS_ROW
#define _IGNITE_CACHE_QUERY_QUERY_FIELDS_ROW

#include <vector>

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include "ignite/cache/cache_entry.h"
#include "ignite/impl/cache/query/query_fields_row_impl.h"
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
             * This class is implemented as a reference to an implementation so copying
             * of this class instance will only create another reference to the same
             * underlying object. Underlying object will be released automatically once all
             * the instances are destructed.
             */
            class QueryFieldsRow
            {
            public:
                /**
                 * Default constructor.
                 *
                 * Constructed instance is not valid and thus can not be used.
                 */
                QueryFieldsRow() : impl(0)
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
                QueryFieldsRow(impl::cache::query::QueryFieldsRowImpl* impl) : impl(impl)
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
                    impl::cache::query::QueryFieldsRowImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->HasNext();
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
                 * Template argument type should be default-constructable,
                 * copy-constructable and assignable. Also BinaryType class
                 * template should be specialized for this type.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @return Next entry.
                 *
                 * @throw IgniteError class instance in case of failure.
                 */
                template<typename T>
                T GetNext()
                {
                    IgniteError err;

                    T res = GetNext<T>(err);

                    IgniteError::ThrowIfNeeded(err);

                    return res;
                }

                /**
                 * Get next entry.
                 * Properly sets error param in case of failure.
                 *
                 * Template argument type should be default-constructable,
                 * copy-constructable and assignable. Also BinaryType class
                 * template should be specialized for this type.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @param err Used to set operation result.
                 * @return Next entry on success and default-constructed type
                 * instance on failure.
                 */
                template<typename T>
                T GetNext(IgniteError& err)
                {
                    impl::cache::query::QueryFieldsRowImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->GetNext<T>(err);
                    else
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Instance is not usable (did you check for error?).");

                        return T();
                    }
                }

                /**
                 * Get next entry assuming it's an array of 8-byte signed
                 * integers. Maps to "byte[]" type in Java.
                 *
                 * This method should only be used on the valid instance.
                 *
                 * @param dst Array to store data to.
                 * @param len Expected length of array.
                 * @return Actual amount of elements read. If "len" argument is less than actual
                 *     array size or resulting array is set to null, nothing will be written
                 *     to resulting array and returned value will contain required array length.
                 *     -1 will be returned in case array in stream was null.
                 *
                 * @throw IgniteError class instance in case of failure.
                 */
                int32_t GetNextInt8Array(int8_t* dst, int32_t len)
                {
                    impl::cache::query::QueryFieldsRowImpl* impl0 = impl.Get();

                    if (impl0)
                        return impl0->GetNextInt8Array(dst, len);
                    else
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Instance is not usable (did you check for error?).");
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
                ignite::common::concurrent::SharedPointer<impl::cache::query::QueryFieldsRowImpl> impl;
            };
        }
    }    
}

#endif //_IGNITE_CACHE_QUERY_QUERY_FIELDS_ROW
