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
 * Declares ignite::impl::compute::ComputeTaskHolder.
 */

#ifndef _IGNITE_IMPL_COMPUTE_COMPUTE_TASK_HOLDER
#define _IGNITE_IMPL_COMPUTE_COMPUTE_TASK_HOLDER

#include <stdint.h>

#include <ignite/impl/compute/compute_job_holder.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            /**
             * Compute task holder. Internal helper class.
             * Used to handle tasks in general way, without specific types.
             */
            class ComputeTaskHolder
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param handle Job handle.
                 */
                ComputeTaskHolder(int64_t handle) :
                    handle(handle)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskHolder()
                {
                    // No-op.
                }

                /**
                 * Process local job result.
                 *
                 * @param job Job.
                 * @return Policy.
                 */
                virtual int32_t JobResultLocal(ComputeJobHolder& job) = 0;

                /**
                 * Process remote job result.
                 *
                 * @param job Job.
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(ComputeJobHolder& job, binary::BinaryReaderImpl& reader) = 0;

                /**
                 * Reduce results of related jobs.
                 */
                virtual void Reduce() = 0;

                /**
                 * Get related job handle.
                 *
                 * @return Job handle.
                 */
                int64_t GetJobHandle()
                {
                    return handle;
                }

            private:
                /** Related job handle. */
                int64_t handle;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_TASK_HOLDER
