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
