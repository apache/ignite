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

#ifndef _IGNITE_CACHE_QUERY_BATCH
#define _IGNITE_CACHE_QUERY_BATCH

#include <cassert>

#include "ignite/ignite_error.h"
#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/operations.h"

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                class QueryFieldsRowImpl;

                /**
                 * Query batch.
                 */
                class IGNITE_IMPORT_EXPORT QueryBatch
                {
                    typedef common::concurrent::SharedPointer<interop::InteropMemory> MemorySharedPtr;

                public:
                    /**
                     * Constructor.
                     *
                     * @param env Environment.
                     * @param mem Batch memory.
                     */
                    QueryBatch(IgniteEnvironment& env, MemorySharedPtr mem) :
                        env(env),
                        mem(mem),
                        stream(mem.Get()),
                        reader(&stream),
                        size(reader.ReadInt32()),
                        pos(0)
                    {
                        // No-op.
                    }

                    /**
                     * Destructor.
                     */
                    ~QueryBatch()
                    {
                        // No-op.
                    }

                    /**
                     * Check whether batch is empty.
                     *
                     * @return True if empty.
                     */
                    bool IsEmpty() const
                    {
                        return size == 0;
                    }

                    /**
                     * Get the number of the unread rows in the batch.
                     *
                     * @return Number of the unread rows in the batch.
                     */
                    int32_t Left() const
                    {
                        return size - pos;
                    }

                    /**
                     * Check whether next result exists.
                     *
                     * @param err Error.
                     * @return True if exists.
                     */
                    int32_t Size()
                    {
                        return size;
                    }

                    /**
                     * Get next object.
                     * 
                     * @param op Operation.
                     */
                    void GetNext(OutputOperation& op)
                    {
                        assert(Left() > 0);

                        op.ProcessOutput(reader);

                        ++pos;
                    }

                    /**
                     * Get next row.
                     *
                     * @return Output row.
                     */
                    QueryFieldsRowImpl* GetNextRow();

                private:
                    /** Environment. */
                    IgniteEnvironment& env;

                    /** Memomy containing the batch. */
                    MemorySharedPtr mem;

                    /** Stream. */
                    interop::InteropInputStream stream;

                    /** Reader. */
                    binary::BinaryReaderImpl reader;

                    /** Result batch size. */
                    int32_t size;

                    /** Position in memory. */
                    int32_t pos;

                    IGNITE_NO_COPY_ASSIGNMENT(QueryBatch);
                };
            }
        }
    }
}

#endif // _IGNITE_CACHE_QUERY_BATCH
