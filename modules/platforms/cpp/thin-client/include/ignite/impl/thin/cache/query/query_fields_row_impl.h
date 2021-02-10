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

#ifndef _IGNITE_IMPL_CACHE_QUERY_CACHE_QUERY_FIELDS_ROW_IMPL
#define _IGNITE_IMPL_CACHE_QUERY_CACHE_QUERY_FIELDS_ROW_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                /**
                 * Query fields cursor implementation.
                 */
                class IGNITE_IMPORT_EXPORT QueryFieldsRowImpl
                {
                public:
                    typedef common::concurrent::SharedPointer<interop::InteropMemory> SP_InteropMemory;

                    /**
                     * Constructor.
                     *
                     * @param mem Memory containig row data.
                     */
                    QueryFieldsRowImpl(SP_InteropMemory mem, int32_t rowBegin, int32_t columnNum) :
                        mem(mem),
                        stream(mem.Get()),
                        reader(&stream),
                        columnNum(columnNum),
                        processed(0)
                    {
                        stream.Position(rowBegin);
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
                        if (IsValid())
                            return processed < columnNum;
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
                    template<typename T>
                    T GetNext()
                    {
                        IgniteError err;

                        QueryFieldsRowImpl res = GetNext<T>(err);

                        IgniteError::ThrowIfNeeded(err);

                        return res;
                    }

                    /**
                     * Get next entry.
                     *
                     * @param err Error.
                     * @return Next entry.
                     */
                    template<typename T>
                    T GetNext(IgniteError& err)
                    {
                        if (IsValid()) {
                            ++processed;
                            return reader.ReadTopObject<T>();
                        }
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
                     * @param dst Array to store data to.
                     * @param len Expected length of array.
                     * @return Actual amount of elements read. If "len" argument is less than actual
                     *     array size or resulting array is set to null, nothing will be written
                     *     to resulting array and returned value will contain required array length.
                     *     -1 will be returned in case array in stream was null.
                     */
                    int32_t GetNextInt8Array(int8_t* dst, int32_t len)
                    {
                        if (IsValid()) {

                            int32_t actualLen = reader.ReadInt8Array(dst, len);

                            if (actualLen == 0 || (dst && len >= actualLen))
                                ++processed;

                            return actualLen;
                        }
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
                    bool IsValid()
                    {
                        return mem.Get() != 0;
                    }

                private:
                    /** Row memory. */
                    SP_InteropMemory mem;

                    /** Row data stream. */
                    interop::InteropInputStream stream;

                    /** Row data reader. */
                    binary::BinaryReaderImpl reader;

                    /** Number of elements in a row. */
                    int32_t columnNum;

                    /** Number of elements that have been read by now. */
                    int32_t processed;

                    IGNITE_NO_COPY_ASSIGNMENT(QueryFieldsRowImpl);
                };
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_CACHE_QUERY_FIELDS_ROW_IMPL
