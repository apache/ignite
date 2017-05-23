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

#include <memory>
#include <sstream>

#include <ignite/common/promise.h>

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
             * Used to hold compute job result.
             */
            template<typename R>
            class ComputeJobResult
            {
            public:
                typedef R ResultType;
                /**
                 * Default constructor.
                 */
                ComputeJobResult() :
                    res(),
                    err()
                {
                    // No-op.
                }

                /**
                 * Set result value.
                 *
                 * @param val Value to set as a result.
                 */
                void SetResult(const ResultType& val)
                {
                    res = val;
                }

                /**
                 * Set error.
                 *
                 * @param error Error to set.
                 */
                void SetError(const IgniteError error)
                {
                    err = error;
                }

                /**
                 * Set promise to a state which corresponds to result.
                 *
                 * @param promise Promise, which state to set.
                 */
                void SetPromise(common::Promise<ResultType>& promise)
                {
                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                        promise.SetError(err);
                    else
                        promise.SetValue(std::auto_ptr<ResultType>(new ResultType(res)));
                }

                /**
                 * Write using writer.
                 *
                 * @param writer Writer.
                 */
                void Write(binary::BinaryWriterImpl& writer)
                {
                    if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
                    {
                        // Fail
                        writer.WriteBool(false);

                        // Native Exception
                        writer.WriteBool(true);

                        writer.WriteObject<IgniteError>(err);
                    }
                    else
                    {
                        // Success
                        writer.WriteBool(true);

                        writer.WriteObject<ResultType>(res);
                    }
                }

                /**
                 * Read using reader.
                 *
                 * @param reader Reader.
                 */
                void Read(binary::BinaryReaderImpl& reader)
                {
                    bool success = reader.ReadBool();

                    if (success)
                    {
                        res = reader.ReadObject<ResultType>();

                        err = IgniteError();
                    }
                    else
                    {
                        bool native = reader.ReadBool();

                        if (native)
                            err = reader.ReadObject<IgniteError>();
                        else
                        {
                            std::stringstream buf;

                            buf << reader.ReadObject<std::string>() << " : ";
                            buf << reader.ReadObject<std::string>() << ", ";
                            buf << reader.ReadObject<std::string>();

                            std::string msg = buf.str();

                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, msg.c_str());
                        }
                    }
                }

            private:
                /** Result. */
                ResultType res;

                /** Erorr. */
                IgniteError err;
            };

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
                virtual void ExecuteLocal() = 0;

                /**
                 * Execute job remote.
                 *
                 * @param writer Writer.
                 */
                virtual void ExecuteRemote(binary::BinaryWriterImpl& writer) = 0;
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

                virtual void ExecuteLocal()
                {
                    try
                    {
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

                virtual void ExecuteRemote(binary::BinaryWriterImpl& writer)
                {
                    ExecuteLocal();

                    res.Write(writer);
                }

            private:
                /** Result. */
                ComputeJobResult<ResultType> res;

                /** Job. */
                JobType job;
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
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_TASK_IMPL
