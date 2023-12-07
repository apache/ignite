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
 * Declares ignite::thin::compute::ComputeClient class.
 */

#ifndef _IGNITE_THIN_COMPUTE_COMPUTE_CLIENT
#define _IGNITE_THIN_COMPUTE_COMPUTE_CLIENT

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace thin
    {
        namespace compute
        {
            struct ComputeClientFlags
            {
                enum Type
                {
                    NONE = 0,
                    NO_FAILOVER = 1,
                    NO_RESULT_CACHE = 2
                };
            };

            /**
             * Client Compute API.
             *
             * @see IgniteClient::GetCompute()
             *
             * This class is implemented as a reference to an implementation so copying of this class instance will only
             * create another reference to the same underlying object. Underlying object will be released automatically
             * once all the instances are destructed.
             */
            class IGNITE_IMPORT_EXPORT ComputeClient
            {
                typedef common::concurrent::SharedPointer<void> SP_Void;
            public:
                /**
                 * Default constructor.
                 */
                ComputeClient()
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 */
                ComputeClient(const SP_Void& impl) :
                    impl(impl),
                    flags(ComputeClientFlags::NONE),
                    timeout(0)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~ComputeClient()
                {
                    // No-op.
                }

                /**
                 * Executes given Java task by class name.
                 *
                 * @param taskName Java task name.
                 * @param taskArg Argument of task execution of type A.
                 * @return Task result of type @c R.
                 *
                 * @tparam R Type of task result.
                 * @tparam A Type of task argument.
                 */
                template<typename R, typename A>
                R ExecuteJavaTask(const std::string& taskName, const A& taskArg)
                {
                    R result;

                    impl::thin::WritableImpl<A> wrArg(taskArg);
                    impl::thin::ReadableImpl<R> rdResult(result);

                    InternalExecuteJavaTask(taskName, wrArg, rdResult);

                    return result;
                }

                /**
                 * Executes given Java task by class name.
                 *
                 * @param taskName Java task name.
                 * @return Task result of type @c R.
                 *
                 * @tparam R Type of task result.
                 */
                template<typename R>
                R ExecuteJavaTask(const std::string& taskName)
                {
                    R result;
                    int* nullVal = 0;

                    impl::thin::WritableImpl<int*> wrArg(nullVal);
                    impl::thin::ReadableImpl<R> rdResult(result);

                    InternalExecuteJavaTask(taskName, wrArg, rdResult);

                    return result;
                }

                /**
                 * Returns a new instance of ComputeClient with a timeout for all task executions.
                 *
                 * @param timeoutMs Timeout in milliseconds.
                 * @return New ComputeClient instance with timeout.
                 */
                ComputeClient WithTimeout(int64_t timeoutMs)
                {
                    return ComputeClient(impl, flags, timeoutMs);
                }

                /**
                 * Returns a new instance of ComputeClient with disabled failover.
                 * When failover is disabled, compute jobs won't be retried in case of node crashes.
                 *
                 * @return New Compute instance with disabled failover.
                 */
                ComputeClient WithNoFailover()
                {
                    return ComputeClient(impl, flags | ComputeClientFlags::NO_FAILOVER, timeout);
                }

                /**
                 * Returns a new instance of ComputeClient with disabled result cache.
                 *
                 * @return New Compute instance with disabled result cache.
                 */
                ComputeClient WithNoResultCache()
                {
                    return ComputeClient(impl, flags | ComputeClientFlags::NO_RESULT_CACHE, timeout);
                }

            private:
                /**
                 * Constructor.
                 *
                 * @param impl Implementation.
                 * @param flags Flags.
                 * @param timeout Timeout in milliseconds.
                 */
                ComputeClient(const SP_Void& impl, int8_t flags, int64_t timeout) :
                    impl(impl),
                    flags(flags),
                    timeout(timeout)
                {
                    // No-op.
                }

                /**
                 * Execute java task internally.
                 *
                 * @param taskName Task name.
                 * @param wrArg Argument.
                 * @param res Result.
                 */
                void InternalExecuteJavaTask(const std::string& taskName, impl::thin::Writable& wrArg,
                    impl::thin::Readable& res);

                /** Implementation. */
                SP_Void impl;

                /** Flags. */
                int8_t flags;

                /** Timeout. */
                int64_t timeout;
            };
        }
    }
}

#endif // _IGNITE_THIN_COMPUTE_COMPUTE_CLIENT
