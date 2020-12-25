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
 * Declares ignite::impl::compute::ComputeJobHolder class template.
 */

#ifndef _IGNITE_IMPL_COMPUTE_COMPUTE_JOB_HOLDER
#define _IGNITE_IMPL_COMPUTE_COMPUTE_JOB_HOLDER

#include <ignite/impl/binary/binary_writer_impl.h>
#include <ignite/impl/compute/compute_job_result.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            /**
             * Compute job holder. Internal helper class.
             * Used to handle jobs in general way, without specific types.
             */
            class ComputeJobHolder
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~ComputeJobHolder()
                {
                    // No-op.
                }

                /**
                 * Execute job locally.
                 */
                virtual void ExecuteLocal(IgniteEnvironment* env) = 0;

                /**
                 * Execute job remote.
                 *
                 * @param writer Writer.
                 */
                virtual void ExecuteRemote(IgniteEnvironment* env, binary::BinaryWriterImpl& writer) = 0;
            };

            /**
             * Compute job holder. Internal class.
             *
             * @tparam F Actual job type.
             * @tparam R Job return type.
             */
            template<typename F, typename R>
            class ComputeJobHolderImpl : public ComputeJobHolder
            {
            public:
                typedef R ResultType;
                typedef F JobType;

                /**
                 * Constructor.
                 *
                 * @param job Job.
                 */
                ComputeJobHolderImpl(JobType job) :
                    job(job)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeJobHolderImpl()
                {
                    // No-op.
                }

                const ComputeJobResult<ResultType>& GetResult()
                {
                    return res;
                }

                virtual void ExecuteLocal(IgniteEnvironment* env)
                {
                    try
                    {
                        job.SetIgnite(env->GetIgnite());
                        res.SetResult(job.Call());
                    }
                    catch (const IgniteError& err)
                    {
                        res.SetError(err);
                    }
                    catch (const std::exception& err)
                    {
                        res.SetError(IgniteError(IgniteError::IGNITE_ERR_STD, err.what()));
                    }
                    catch (...)
                    {
                        res.SetError(IgniteError(IgniteError::IGNITE_ERR_UNKNOWN,
                            "Unknown error occurred during call."));
                    }
                }

                virtual void ExecuteRemote(IgniteEnvironment* env, binary::BinaryWriterImpl& writer)
                {
                    ExecuteLocal(env);

                    res.Write(writer);
                }

            private:
                /** Result. */
                ComputeJobResult<ResultType> res;

                /** Job. */
                JobType job;
            };

            /**
             * Compute job holder. Internal class.
             * Specialisation for void return type
             *
             * @tparam F Actual job type.
             */
            template<typename F>
            class ComputeJobHolderImpl<F, void> : public ComputeJobHolder
            {
            public:
                typedef F JobType;

                /**
                 * Constructor.
                 *
                 * @param job Job.
                 */
                ComputeJobHolderImpl(JobType job) :
                    job(job)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~ComputeJobHolderImpl()
                {
                    // No-op.
                }

                const ComputeJobResult<void>& GetResult()
                {
                    return res;
                }

                virtual void ExecuteLocal(IgniteEnvironment* env)
                {
                    try
                    {
                        job.SetIgnite(env->GetIgnite());
                        job.Call();
                        res.SetResult();
                    }
                    catch (const IgniteError& err)
                    {
                        res.SetError(err);
                    }
                    catch (const std::exception& err)
                    {
                        res.SetError(IgniteError(IgniteError::IGNITE_ERR_STD, err.what()));
                    }
                    catch (...)
                    {
                        res.SetError(IgniteError(IgniteError::IGNITE_ERR_UNKNOWN,
                            "Unknown error occurred during call."));
                    }
                }

                virtual void ExecuteRemote(IgniteEnvironment* env, binary::BinaryWriterImpl& writer)
                {
                    ExecuteLocal(env);

                    res.Write(writer);
                }

            private:
                /** Result. */
                ComputeJobResult<void> res;

                /** Job. */
                JobType job;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_JOB_HOLDER
