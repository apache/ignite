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
 * Declares ignite::thin::cache::query::QueryFieldsRow class.
 */

#ifndef _IGNITE_THIN_CACHE_QUERY_QUERY_FIELDS_ROW
#define _IGNITE_THIN_CACHE_QUERY_QUERY_FIELDS_ROW

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include <ignite/impl/thin/readable.h>

namespace ignite
{
    namespace thin
    {
        namespace cache
        {
            namespace query
            {
                /**
                 * Query fields row.
                 *
                 * This class is implemented as a reference to an implementation so copying of this class instance will
                 * only create another reference to the same underlying object. Underlying object will be released
                 * automatically once all the instances are destructed.
                 */
                class IGNITE_IMPORT_EXPORT QueryFieldsRow
                {
                public:
                    /**
                     * Constructor.
                     *
                     * Internal method. Should not be used by user.
                     *
                     * @param impl Implementation.
                     */
                    explicit QueryFieldsRow(const common::concurrent::SharedPointer<void>& impl);

                    /**
                     * Check whether next entry exists.
                     *
                     * @return True if next entry exists.
                     */
                    bool HasNext();

                    /**
                     * Get next entry.
                     *
                     * @tparam T Value type to get. Should be default-constructable, copy-constructable and assignable.
                     *     Also BinaryType class template should be specialized for this type.
                     *
                     * @return Next entry.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    template<typename T>
                    T GetNext()
                    {
                        T res;
                        impl::thin::ReadableImpl<T> readable(res);

                        InternalGetNext(readable);

                        return res;
                    }

                private:
                    /**
                     * Get next entry.
                     *
                     * @param readable Value to read.
                     *
                     * @throw IgniteError class instance in case of failure.
                     */
                    void InternalGetNext(impl::thin::Readable& readable);

                    /** Implementation delegate. */
                    common::concurrent::SharedPointer<void> impl;
                };
            }
        }
    }    
}

#endif //_IGNITE_THIN_CACHE_QUERY_QUERY_FIELDS_ROW
