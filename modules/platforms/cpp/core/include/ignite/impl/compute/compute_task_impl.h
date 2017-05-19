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
 * Declares ignite::impl::compute::ComputeTask class template.
 */

#ifndef _IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL
#define _IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL

#include <stdint.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            class ComputeJobResultImpl
            {
            public:
            };

            class ComputeJobHolder
            {
            public:
                const ComputeJobResultImpl& GetResult()
                {
                    return res;
                }

            private:
                ComputeJobResultImpl res;
            };

            class ComputeTaskImplBase
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskImplBase()
                {
                    // No-op.
                }

                /**
                 * Process local job result.
                 *
                 * @param job Job.
                 * @return Policy.
                 */
                int32_t JobResultLocal(ComputeJobHolder job)
                {
                    return 0;
                }

            };

            /**
             * @tparam R Result type.
             */
            template<typename R>
            class ComputeTaskImpl
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskImpl()
                {
                    // No-op.
                }

                /**
                 * Get result promise.
                 *
                 * @return Reference to result promise.
                 */
                common::Promise<R>& GetPromise()
                {
                    return promise;
                }

            private:
                /** Task result promise. */
                common::Promise<R> promise;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL
