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

#include <ignite/ignite_error.h>
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
                 * @param reader Reader for stream with result.
                 * @return Policy.
                 */
                virtual int32_t JobResultRemote(binary::BinaryReaderImpl& reader) = 0;

                /**
                 * Process error.
                 *
                 * @param err Error.
                 */
                virtual void JobResultError(const IgniteError& err) = 0;

                /**
                 * Process successful result.
                 *
                 * @param value Value.
                 */
                virtual void JobResultSuccess(int64_t value) = 0;

                /**
                 * Process successful result.
                 *
                 * @param reader Reader for stream with result.
                 */
                virtual void JobResultSuccess(binary::BinaryReaderImpl& reader) = 0;

                /**
                 * Process successful result.
                 */
                virtual void JobNullResultSuccess() = 0;

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
             * Read future result.
             * @tparam T Type of the result.
             * @param value Value.
             * @return Result.
             */
            template<typename T> T PrimitiveFutureResult(int64_t value)
            {
                IGNITE_ERROR_FORMATTED_1(IgniteError::IGNITE_ERR_GENERIC,
                     "Primitive value passed to non-primitive future", "value", value);
            }

            template<> inline int8_t PrimitiveFutureResult<int8_t>(int64_t value)
            {
                return static_cast<int8_t>(value);
            }

            template<> inline int16_t PrimitiveFutureResult<int16_t>(int64_t value)
            {
                return static_cast<int16_t>(value);
            }

            template<> inline int32_t PrimitiveFutureResult<int32_t>(int64_t value)
            {
                return static_cast<int32_t>(value);
            }

            template<> inline int64_t PrimitiveFutureResult<int64_t>(int64_t value)
            {
                return static_cast<int64_t>(value);
            }

            template<> inline bool PrimitiveFutureResult<bool>(int64_t value)
            {
                return value != 0;
            }

            template<> inline uint16_t PrimitiveFutureResult<uint16_t>(int64_t value)
            {
                return static_cast<uint16_t>(value);
            }

            template<> inline float PrimitiveFutureResult<float>(int64_t value)
            {
                impl::interop::BinaryFloatInt32 u;

                u.i = static_cast<int32_t>(value);
                return u.f;
            }

            template<> inline double PrimitiveFutureResult<double>(int64_t value)
            {
                impl::interop::BinaryDoubleInt64 u;

                u.i = value;
                return u.d;
            }
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_TASK_HOLDER
