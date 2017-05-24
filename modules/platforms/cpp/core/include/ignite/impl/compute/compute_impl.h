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
 * Declares ignite::impl::compute::ComputeImpl class.
 */

#ifndef _IGNITE_IMPL_COMPUTE_COMPUTE_IMPL
#define _IGNITE_IMPL_COMPUTE_COMPUTE_IMPL

#include <ignite/common/common.h>
#include <ignite/common/promise.h>
#include <ignite/impl/interop/interop_target.h>
#include <ignite/impl/compute/compute_task_holder.h>
#include <ignite/impl/compute/cancelable_impl.h>

#include <ignite/ignite_error.h>

namespace ignite
{
    namespace impl
    {
        namespace compute
        {
            /**
             * Compute implementation.
             */
            class IGNITE_IMPORT_EXPORT ComputeImpl : public interop::InteropTarget
            {
            public:
                /**
                 * Operation type.
                 */
                struct Operation
                {
                    enum Type
                    {
                        Unicast = 5
                    };
                };

                /**
                 * Constructor.
                 *
                 * @param env Environment.
                 * @param javaRef Java object reference.
                 */
                ComputeImpl(common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);

                /**
                 * Asyncronuously calls provided ComputeFunc on a node within
                 * the underlying cluster group.
                 *
                 * @tparam F Compute function type. Should implement ComputeFunc
                 *  class.
                 * @tparam R Call return type. BinaryType should be specialized for
                 *  the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::Run().
                 * @param func Compute function to call.
                 * @return Future that can be used to acess computation result once
                 *  it's ready.
                 * @throw IgniteError in case of error.
                 */
                template<typename R, typename F>
                Future<R> CallAsync(const F& func)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(mem.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    common::concurrent::SharedPointer<ComputeJobHolder> job(new ComputeJobHolderImpl<F, R>(func));

                    int64_t jobHandle = GetEnvironment().GetHandleRegistry().Allocate(job);

                    ComputeTaskHolderImpl<F, R>* taskPtr = new ComputeTaskHolderImpl<F, R>(jobHandle);
                    common::concurrent::SharedPointer<ComputeTaskHolder> task(taskPtr);

                    int64_t taskHandle = GetEnvironment().GetHandleRegistry().Allocate(task);

                    writer.WriteInt64(taskHandle);
                    writer.WriteInt32(1);
                    writer.WriteInt64(jobHandle);
                    writer.WriteObject<F>(func);

                    out.Synchronize();

                    jobject target = InStreamOutObject(Operation::Unicast, *mem.Get());
                    std::auto_ptr<common::Cancelable> cancelable(new CancelableImpl(GetEnvironmentPointer(), target));

                    common::Promise<R>& promise = taskPtr->GetPromise();
                    promise.SetCancelTarget(cancelable);

                    return promise.GetFuture();
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ComputeImpl);
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_IMPL
