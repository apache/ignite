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
#include <ignite/impl/compute/single_job_compute_task_holder.h>
#include <ignite/impl/compute/multiple_job_compute_task_holder.h>
#include <ignite/impl/compute/cancelable_impl.h>

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
                        BROADCAST = 2,

                        UNICAST = 5,
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
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @tparam R Call return type. BinaryType should be specialized
                 *  for the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::Run().
                 * @param func Compute function to call.
                 * @return Future that can be used to acess computation result
                 *  once it's ready.
                 */
                template<typename R, typename F>
                Future<R> CallAsync(const F& func)
                {
                    typedef ComputeJobHolderImpl<F, R> JobType;
                    typedef SingleJobComputeTaskHolder<F, R> TaskType;

                    return PerformTask<R, F, JobType, TaskType>(Operation::UNICAST, func);
                }

                /**
                 * Asyncronuously runs provided ComputeFunc on a node within
                 * the underlying cluster group.
                 *
                 * @tparam F Compute action type. Should implement
                 *  ComputeFunc<R> class.
                 * @param action Compute action to call.
                 * @return Future that can be used to wait for action
                 *  to complete.
                 */
                template<typename F>
                Future<void> RunAsync(const F& action)
                {
                    typedef ComputeJobHolderImpl<F, void> JobType;
                    typedef SingleJobComputeTaskHolder<F, void> TaskType;

                    return PerformTask<void, F, JobType, TaskType>(Operation::UNICAST, action);
                }

                /**
                 * Asyncronuously broadcasts provided ComputeFunc to all nodes
                 * in the underlying cluster group.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @tparam R Call return type. BinaryType should be specialized
                 *  for the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::Run().
                 * @param func Compute function to call.
                 * @return Future that can be used to acess computation result
                 *  once it's ready.
                 */
                template<typename R, typename F>
                Future< std::vector<R> > BroadcastAsync(const F& func)
                {
                    typedef ComputeJobHolderImpl<F, R> JobType;
                    typedef MultipleJobComputeTaskHolder<F, R> TaskType;

                    return PerformTask<std::vector<R>, F, JobType, TaskType>(Operation::BROADCAST, func);
                }

                /**
                 * Asyncronuously broadcasts provided ComputeFunc to all nodes
                 * in the underlying cluster group.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @param func Compute function to call.
                 * @return Future that can be used to acess computation result
                 *  once it's ready.
                 */
                template<typename F, bool>
                Future<void> BroadcastAsync(const F& func)
                {
                    typedef ComputeJobHolderImpl<F, void> JobType;
                    typedef MultipleJobComputeTaskHolder<F, void> TaskType;

                    return PerformTask<void, F, JobType, TaskType>(Operation::BROADCAST, func);
                }

            private:
                /**
                 * Perform job.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @tparam R Call return type. BinaryType should be specialized
                 *  for the type if it is not primitive.
                 * @tparam J Job type.
                 * @tparam T Task type.
                 *
                 * @param operation Operation type.
                 * @param func Function.
                 * @return Future that can be used to acess computation result
                 *  once it's ready.
                 */
                template<typename R, typename F, typename J, typename T>
                Future<R> PerformTask(Operation::Type operation, const F& func)
                {
                    common::concurrent::SharedPointer<ComputeJobHolder> job(new J(func));

                    int64_t jobHandle = GetEnvironment().GetHandleRegistry().Allocate(job);

                    T* taskPtr = new T(jobHandle);
                    common::concurrent::SharedPointer<ComputeTaskHolder> task(taskPtr);

                    int64_t taskHandle = GetEnvironment().GetHandleRegistry().Allocate(task);

                    std::auto_ptr<common::Cancelable> cancelable = PerformTask(operation, jobHandle, taskHandle, func);

                    common::Promise<R>& promise = taskPtr->GetPromise();
                    promise.SetCancelTarget(cancelable);

                    return promise.GetFuture();
                }

                /**
                 * Perform job.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 *
                 * @param operation Operation type.
                 * @param jobHandle Job Handle.
                 * @param taskHandle Task Handle.
                 * @param func Function.
                 * @return Cancelable auto pointer.
                 */
                template<typename F>
                std::auto_ptr<common::Cancelable> PerformTask(Operation::Type operation, int64_t jobHandle,
                    int64_t taskHandle, const F& func)
                {
                    common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(mem.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteInt64(taskHandle);
                    writer.WriteInt32(1);
                    writer.WriteInt64(jobHandle);
                    writer.WriteObject<F>(func);

                    out.Synchronize();

                    IgniteError err;
                    jobject target = InStreamOutObject(operation, *mem.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    std::auto_ptr<common::Cancelable> cancelable(new CancelableImpl(GetEnvironmentPointer(), target));

                    return cancelable;
                }

                IGNITE_NO_COPY_ASSIGNMENT(ComputeImpl);
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_IMPL
