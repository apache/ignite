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
 * Declares ignite::impl::compute::ComputeTaskHolder class and
 * ignite::impl::compute::ComputeTaskHolderImpl class template.
 */

#ifndef _IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL
#define _IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL

#include <stdint.h>

#include <ignite/common/promise.h>
#include <ignite/impl/compute/compute_job_result.h>
#include <ignite/impl/compute/compute_job_holder.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            struct ComputeJobResultPolicy
            {
                enum Type
                {
                    /**
                     * Wait for results if any are still expected. If all results have been received -
                     * it will start reducing results.
                     */
                    WAIT = 0,

                    /**
                     * Ignore all not yet received results and start reducing results.
                     */
                    REDUCE = 1,

                    /**
                     * Fail-over job to execute on another node.
                     */
                    FAILOVER = 2
                };
            };

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

            /**
             * Compute task holder type-specific implementation.
             */
            template<typename F, typename R>
            class ComputeTaskHolderImpl : public ComputeTaskHolder
            {
            public:
                typedef F JobType;
                typedef R ResultType;

                /**
                 * Constructor.
                 *
                 * @param handle Job handle.
                 */
                ComputeTaskHolderImpl(int64_t handle) :
                    ComputeTaskHolder(handle)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskHolderImpl()
                {
                    // No-op.
                }

                /**
                 * Process local job result.
                 *
                 * @param job Job.
                 * @return Policy.
                 */
                virtual int32_t JobResultLocal(ComputeJobHolder& job)
                {
                    typedef ComputeJobHolderImpl<JobType, ResultType> ActualComputeJobHolder;

                    ActualComputeJobHolder& job0 = static_cast<ActualComputeJobHolder&>(job);

                    res = job0.GetResult();

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param job Job.
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(ComputeJobHolder& job, binary::BinaryReaderImpl& reader)
                {
                    res.Read(reader);

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Reduce results of related jobs.
                 */
                virtual void Reduce()
                {
                    res.SetPromise(promise);
                }

                /**
                 * Get result promise.
                 *
                 * @return Reference to result promise.
                 */
                common::Promise<ResultType>& GetPromise()
                {
                    return promise;
                }

            private:
                /** Result. */
                ComputeJobResult<ResultType> res;

                /** Task result promise. */
                common::Promise<ResultType> promise;
            };

            /**
             * Compute task holder type-specific implementation.
             */
            template<typename F>
            class ComputeTaskHolderImpl<F, void> : public ComputeTaskHolder
            {
            public:
                typedef F JobType;

                /**
                 * Constructor.
                 *
                 * @param handle Job handle.
                 */
                ComputeTaskHolderImpl(int64_t handle) :
                    ComputeTaskHolder(handle)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeTaskHolderImpl()
                {
                    // No-op.
                }

                /**
                 * Process local job result.
                 *
                 * @param job Job.
                 * @return Policy.
                 */
                virtual int32_t JobResultLocal(ComputeJobHolder& job)
                {
                    typedef ComputeJobHolderImpl<JobType, void> ActualComputeJobHolder;

                    ActualComputeJobHolder& job0 = static_cast<ActualComputeJobHolder&>(job);

                    res = job0.GetResult();

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param job Job.
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(ComputeJobHolder& job, binary::BinaryReaderImpl& reader)
                {
                    res.Read(reader);

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Reduce results of related jobs.
                 */
                virtual void Reduce()
                {
                    res.SetPromise(promise);
                }

                /**
                 * Get result promise.
                 *
                 * @return Reference to result promise.
                 */
                common::Promise<void>& GetPromise()
                {
                    return promise;
                }

            private:
                /** Result. */
                ComputeJobResult<void> res;

                /** Task result promise. */
                common::Promise<void> promise;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL
