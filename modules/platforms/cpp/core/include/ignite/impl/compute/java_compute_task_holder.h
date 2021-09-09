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
 * Declares ignite::impl::compute::JavaComputeTaskHolder class template.
 */

#ifndef _IGNITE_IMPL_COMPUTE_JAVA_COMPUTE_TASK_HOLDER
#define _IGNITE_IMPL_COMPUTE_JAVA_COMPUTE_TASK_HOLDER

#include <stdint.h>

#include <ignite/common/promise.h>
#include <ignite/impl/compute/compute_job_result.h>
#include <ignite/impl/compute/compute_task_holder.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            /**
             * Compute task holder type-specific implementation.
             */
            template<typename R>
            class JavaComputeTaskHolder : public ComputeTaskHolder
            {
            public:
                typedef R ResultType;

                /**
                 * Constructor.
                 */
                JavaComputeTaskHolder() :
                    ComputeTaskHolder(-1)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~JavaComputeTaskHolder()
                {
                    // No-op.
                }

                /**
                 * Process local job result.
                 *
                 * @param job Job.
                 * @return Policy.
                 */
                virtual int32_t JobResultLocal(ComputeJobHolder&)
                {
                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(binary::BinaryReaderImpl&)
                {
                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultError(const IgniteError& err)
                {
                    res.SetError(err);
                }

                /**
                 * Process successful result.
                 *
                 * @param value Value.
                 */
                virtual void JobResultSuccess(int64_t value)
                {
                    res.SetResult(PrimitiveFutureResult<ResultType>(value));
                }

                /**
                 * Process successful result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultSuccess(binary::BinaryReaderImpl& reader)
                {
                    res.SetResult(reader.ReadObject<ResultType>());
                }

                /**
                 * Process successful null result.
                 */
                virtual void JobNullResultSuccess()
                {
                    res.SetResult(impl::binary::BinaryUtils::GetDefaultValue<ResultType>());
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
            template<>
            class JavaComputeTaskHolder<void> : public ComputeTaskHolder
            {
            public:
                /**
                 * Constructor.
                 */
                JavaComputeTaskHolder() :
                    ComputeTaskHolder(-1)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~JavaComputeTaskHolder()
                {
                    // No-op.
                }

                /**
                 * Process local job result.
                 *
                 * @param job Job.
                 * @return Policy.
                 */
                virtual int32_t JobResultLocal(ComputeJobHolder&)
                {
                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(binary::BinaryReaderImpl&)
                {
                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultError(const IgniteError& err)
                {
                    res.SetError(err);
                }

                /**
                 * Process successful result.
                 *
                 * @param value Value.
                 */
                virtual void JobResultSuccess(int64_t)
                {
                    res.SetResult();
                }

                /**
                 * Process successful result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultSuccess(binary::BinaryReaderImpl&)
                {
                    res.SetResult();
                }

                /**
                 * Process successful null result.
                 */
                virtual void JobNullResultSuccess()
                {
                    res.SetResult();
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

#endif //_IGNITE_IMPL_COMPUTE_JAVA_COMPUTE_TASK_HOLDER
