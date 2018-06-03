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

#ifndef _IGNITE_IMPL_CACHE_QUERY_QUERY_IMPL
#define _IGNITE_IMPL_CACHE_QUERY_QUERY_IMPL

#include <ignite/ignite_error.h>

#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/operations.h"
#include "ignite/impl/cache/query/query_batch.h"

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
                 * Query cursor implementation.
                 */
                class IGNITE_IMPORT_EXPORT QueryCursorImpl
                {
                public:
                    /**
                     * Constructor.
                     * 
                     * @param env Environment.
                     * @param javaRef Java reference.
                     */
                    QueryCursorImpl(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);

                    /**
                     * Destructor.
                     */
                    ~QueryCursorImpl();

                    /**
                     * Check whether next result exists.
                     *
                     * @param err Error.
                     * @return True if exists.
                     */
                    bool HasNext(IgniteError& err);

                    /**
                     * Get next object.
                     * 
                     * @param op Operation.
                     * @param err Error.
                     */
                    void GetNext(OutputOperation& op, IgniteError& err);

                    /**
                     * Get next row.
                     *
                     * @param err Error.
                     * @return Output row.
                     */
                    QueryFieldsRowImpl* GetNextRow(IgniteError& err);

                    /**
                     * Get all cursor entries.
                     *
                     * @param op Operation.
                     * @param err Error.
                     */
                    void GetAll(OutputOperation& op, IgniteError& err);

                    /**
                     * Get all cursor entries.
                     *
                     * @param op Operation.
                     */
                    void GetAll(OutputOperation& op);

                private:
                    /** Environment. */
                    ignite::common::concurrent::SharedPointer<impl::IgniteEnvironment> env;

                    /** Handle to Java object. */
                    jobject javaRef;

                    /** Current result batch. */
                    QueryBatch* batch;

                    /** Whether cursor has no more elements available. */
                    bool endReached;

                    /** Whether iteration methods were called. */
                    bool iterCalled;

                    /** Whether GetAll() method was called. */
                    bool getAllCalled;

                    IGNITE_NO_COPY_ASSIGNMENT(QueryCursorImpl);

                    /**
                     * Create Java-side iterator if needed.
                     *
                     * @param err Error.
                     * @return True in case of success, false if an error is thrown.
                     */
                    bool CreateIteratorIfNeeded(IgniteError& err);

                   /**
                     * Get next result batch if update is needed.
                     *
                     * @param err Error.
                     * @return True if operation has been successful.
                     */
                    bool GetNextBatchIfNeeded(IgniteError& err);

                    /**
                     * Check whether Java-side iterator has next element.
                     *
                     * @param err Error.
                     * @return True if the next element is available.
                     */
                    bool IteratorHasNext(IgniteError& err);
                };
            }
        }
    }
}

#endif //_IGNITE_IMPL_CACHE_QUERY_QUERY_IMPL