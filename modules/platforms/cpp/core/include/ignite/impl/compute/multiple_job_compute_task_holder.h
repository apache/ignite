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
 * Declares ignite::impl::compute::MultipleJobComputeTaskHolder class template.
 */

#ifndef _IGNITE_IMPL_COMPUTE_MULTIPLE_JOB_COMPUTE_TASK
#define _IGNITE_IMPL_COMPUTE_MULTIPLE_JOB_COMPUTE_TASK

#include <stdint.h>
#include <vector>

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
             * Multiple Job Compute task holder type-specific implementation.
             * Used for broadcast.
             *
             * @tparam F Function type.
             * @tparam R Function result type.
             */
            template<typename F, typename R>
            class MultipleJobComputeTaskHolder : public ComputeTaskHolder
            {
            public:
                typedef F JobType;
                typedef R ResultType;

                /**
                 * Constructor.
                 *
                 * @param handle Job handle.
                 */
                MultipleJobComputeTaskHolder(int64_t handle) :
                    ComputeTaskHolder(handle),
                    result(new std::vector<ResultType>()),
                    error(),
                    promise()
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~MultipleJobComputeTaskHolder()
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

                    ProcessResult(job0.GetResult());

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(binary::BinaryReaderImpl& reader)
                {
                    ComputeJobResult<ResultType> res;

                    res.Read(reader);

                    ProcessResult(res);

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultError(const IgniteError& err)
                {
                    ComputeJobResult<ResultType> res;

                    res.SetError(err);

                    ProcessResult(res);
                }

                /**
                 * Process successful result.
                 *
                 * @param value Value.
                 */
                virtual void JobResultSuccess(int64_t value)
                {
                    ComputeJobResult<ResultType> res;

                    res.SetResult(PrimitiveFutureResult<ResultType>(value));

                    ProcessResult(res);
                }

                /**
                 * Process successful result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultSuccess(binary::BinaryReaderImpl& reader)
                {
                    ComputeJobResult<ResultType> res;

                    res.SetResult(reader.ReadObject<ResultType>());

                    ProcessResult(res);
                }

                /**
                 * Process successful null result.
                 */
                virtual void JobNullResultSuccess()
                {
                    ComputeJobResult<ResultType> res;

                    res.SetResult(impl::binary::BinaryUtils::GetDefaultValue<ResultType>());

                    ProcessResult(res);
                }

                /**
                 * Reduce results of related jobs.
                 */
                virtual void Reduce()
                {
                    if (error.GetCode() == IgniteError::IGNITE_SUCCESS)
                        promise.SetValue(result);
                    else
                        promise.SetError(error);
                }

                /**
                 * Get result promise.
                 *
                 * @return Reference to result promise.
                 */
                common::Promise< std::vector<ResultType> >& GetPromise()
                {
                    return promise;
                }

            private:
                /**
                 * Process result.
                 *
                 * @param res Result.
                 */
                void ProcessResult(const ComputeJobResult<ResultType>& res)
                {
                    const IgniteError& err = res.GetError();

                    if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                        result->push_back(res.GetResult());
                    else
                        error = err;
                }

                /** Result. */
                std::auto_ptr< std::vector<ResultType> > result;

                /** Error. */
                IgniteError error;

                /** Task result promise. */
                common::Promise< std::vector<ResultType> > promise;
            };

            /**
             * Compute task holder type-specific implementation.
             */
            template<typename F>
            class MultipleJobComputeTaskHolder<F, void> : public ComputeTaskHolder
            {
            public:
                typedef F JobType;

                /**
                 * Constructor.
                 *
                 * @param handle Job handle.
                 */
                MultipleJobComputeTaskHolder(int64_t handle) :
                    ComputeTaskHolder(handle)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~MultipleJobComputeTaskHolder()
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

                    ProcessResult(job0.GetResult());

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(binary::BinaryReaderImpl& reader)
                {
                    ComputeJobResult<void> res;

                    res.Read(reader);

                    ProcessResult(res);

                    return ComputeJobResultPolicy::WAIT;
                }

                /**
                 * Process remote job result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultError(const IgniteError& err)
                {
                    ComputeJobResult<void> res;

                    res.SetError(err);

                    ProcessResult(res);
                }

                /**
                 * Process successful result.
                 *
                 * @param value Value.
                 */
                virtual void JobResultSuccess(int64_t)
                {
                    ComputeJobResult<void> res;

                    res.SetResult();

                    ProcessResult(res);
                }

                /**
                 * Process successful result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultSuccess(binary::BinaryReaderImpl&)
                {
                    ComputeJobResult<void> res;

                    res.SetResult();

                    ProcessResult(res);
                }

                /**
                 * Process successful null result.
                 */
                virtual void JobNullResultSuccess()
                {
                    ComputeJobResult<void> res;

                    res.SetResult();

                    ProcessResult(res);
                }

                /**
                 * Reduce results of related jobs.
                 */
                virtual void Reduce()
                {
                    if (error.GetCode() == IgniteError::IGNITE_SUCCESS)
                        promise.SetValue();
                    else
                        promise.SetError(error);
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
                /**
                 * Process result.
                 *
                 * @param res Result.
                 */
                void ProcessResult(const ComputeJobResult<void>& res)
                {
                    const IgniteError& err = res.GetError();

                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                        error = err;
                }

                /** Error. */
                IgniteError error;

                /** Task result promise. */
                common::Promise<void> promise;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_MULTIPLE_JOB_COMPUTE_TASK
