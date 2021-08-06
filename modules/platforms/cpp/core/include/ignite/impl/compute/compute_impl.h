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
#include <ignite/impl/compute/java_compute_task_holder.h>
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

                        EXEC = 3,

                        EXEC_ASYNC = 4,

                        UNICAST = 5,

                        AFFINITY_CALL = 13,

                        AFFINITY_RUN = 14
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
                 * Executes given job asynchronously on the node where data for
                 * provided affinity key is located (a.k.a. affinity co-location).
                 *
                 * @tparam R Call return type. BinaryType should be specialized for
                 *  the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::AffinityRun().
                 * @tparam K Affinity key type.
                 * @tparam F Compute function type. Should implement ComputeFunc<R>
                 *  class.
                 * @param cacheName Cache name to use for affinity co-location.
                 * @param key Affinity key.
                 * @param func Compute function to call.
                 * @return Future that can be used to access computation result once
                 *  it's ready.
                 * @throw IgniteError in case of error.
                 */
                template<typename R, typename K, typename F>
                Future<R> AffinityCallAsync(const std::string& cacheName, const K& key, const F& func)
                {
                    typedef ComputeJobHolderImpl<F, R> JobType;
                    typedef SingleJobComputeTaskHolder<F, R> TaskType;

                    return PerformAffinityTask<R, K, F, JobType, TaskType>(cacheName, key, func, Operation::AFFINITY_CALL);
                }

                /**
                 * Executes given job asynchronously on the node where data for
                 * provided affinity key is located (a.k.a. affinity co-location).
                 *
                 * @tparam K Affinity key type.
                 * @tparam F Compute function type. Should implement ComputeFunc<R>
                 *  class.
                 * @param cacheName Cache names to use for affinity co-location.
                 * @param key Affinity key.
                 * @param action Compute action to call.
                 * @return Future that can be used to access computation result once
                 *  it's ready.
                 * @throw IgniteError in case of error.
                 */
                template<typename K, typename F>
                Future<void> AffinityRunAsync(const std::string& cacheName, const K& key, const F& action)
                {
                    typedef ComputeJobHolderImpl<F, void> JobType;
                    typedef SingleJobComputeTaskHolder<F, void> TaskType;

                    return PerformAffinityTask<void, K, F, JobType, TaskType>(cacheName, key, action, Operation::AFFINITY_RUN);
                }

                /**
                 * Asynchronously calls provided ComputeFunc on a node within
                 * the underlying cluster group.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @tparam R Call return type. BinaryType should be specialized
                 *  for the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::Run().
                 * @param func Compute function to call.
                 * @return Future that can be used to access computation result
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
                 * Asynchronously runs provided ComputeFunc on a node within
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
                 * Asynchronously broadcasts provided ComputeFunc to all nodes
                 * in the underlying cluster group.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @tparam R Call return type. BinaryType should be specialized
                 *  for the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::Run().
                 * @param func Compute function to call.
                 * @return Future that can be used to access computation result
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
                 * Asynchronously broadcasts provided ComputeFunc to all nodes
                 * in the underlying cluster group.
                 *
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @param func Compute function to call.
                 * @return Future that can be used to access computation result
                 *  once it's ready.
                 */
                template<typename F, bool>
                Future<void> BroadcastAsync(const F& func)
                {
                    typedef ComputeJobHolderImpl<F, void> JobType;
                    typedef MultipleJobComputeTaskHolder<F, void> TaskType;

                    return PerformTask<void, F, JobType, TaskType>(Operation::BROADCAST, func);
                }

                /**
                 * Asynchronously executes given Java task on the grid projection. If task for given name has not been
                 * deployed yet, then 'taskName' will be used as task class name to auto-deploy the task.
                 *
                 * @param taskName Java task name.
                 * @param taskArg Argument of task execution of type A.
                 * @return Future containing a result of type @c R.
                 *
                 * @tparam R Type of task result.
                 * @tparam A Type of task argument.
                 */
                template<typename R, typename A>
                Future<R> ExecuteJavaTaskAsync(const std::string& taskName, const A& taskArg)
                {
                    return PerformJavaTaskAsync<R, A>(Operation::EXEC_ASYNC, taskName, &taskArg);
                }

                /**
                 * Asynchronously executes given Java task on the grid projection. If task for given name has not been
                 * deployed yet, then 'taskName' will be used as task class name to auto-deploy the task.
                 *
                 * @param taskName Java task name.
                 * @return Future containing a result of type @c R.
                 *
                 * @tparam R Type of task result.
                 */
                template<typename R>
                Future<R> ExecuteJavaTaskAsync(const std::string& taskName)
                {
                    return PerformJavaTaskAsync<R, void>(Operation::EXEC_ASYNC, taskName, 0);
                }

            private:
                struct FutureType
                {
                    enum Type
                    {
                        BYTE = 1,
                        BOOL = 2,
                        SHORT = 3,
                        CHAR = 4,
                        INT = 5,
                        FLOAT = 6,
                        LONG = 7,
                        DOUBLE = 8,
                        OBJECT = 9,
                    };
                };

                template<typename T> struct FutureTypeForType { static const int32_t value = FutureType::OBJECT; };

                template<> struct FutureTypeForType<int8_t> { static const int32_t value = FutureType::BYTE; };
                template<> struct FutureTypeForType<bool> { static const int32_t value = FutureType::BOOL; };
                template<> struct FutureTypeForType<int16_t> { static const int32_t value = FutureType::SHORT; };
                template<> struct FutureTypeForType<uint16_t> { static const int32_t value = FutureType::CHAR; };
                template<> struct FutureTypeForType<int32_t> { static const int32_t value = FutureType::INT; };
                template<> struct FutureTypeForType<int64_t> { static const int32_t value = FutureType::LONG; };
                template<> struct FutureTypeForType<float> { static const int32_t value = FutureType::FLOAT; };
                template<> struct FutureTypeForType<double> { static const int32_t value = FutureType::DOUBLE; };

                template<typename R, typename A>
                Future<R> PerformJavaTaskAsync(Operation::Type operation, const std::string& taskName, const A* arg)
                {
                    typedef JavaComputeTaskHolder<R> TaskHolder;
                    common::concurrent::SharedPointer<TaskHolder> task(new TaskHolder());
                    int64_t taskHandle = GetEnvironment().GetHandleRegistry().Allocate(task);

                    common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(mem.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    writer.WriteString(taskName);
                    // Keep binary
                    writer.WriteBool(false);
                    if (arg)
                        writer.WriteObject<A>(*arg);
                    else
                        writer.WriteNull();
                    // TODO: Node IDs go here
                    writer.WriteBool(false);

                    writer.WriteInt64(taskHandle);
                    writer.WriteInt32(FutureTypeForType<R>::value);

                    out.Synchronize();

                    IgniteError err;
                    jobject target = InStreamOutObject(operation, *mem.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    std::auto_ptr<common::Cancelable> cancelable(new CancelableImpl(GetEnvironmentPointer(), target));

                    common::Promise<R>& promise = task.Get()->GetPromise();
                    promise.SetCancelTarget(cancelable);

                    return promise.GetFuture();
                }

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
                 * @return Future that can be used to access computation result
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

                /**
                 * Perform job in case of cache affinity.
                 *
                 * @tparam R Call return type. BinaryType should be specialized for
                 *  the type if it is not primitive. Should not be void. For
                 *  non-returning methods see Compute::AffinityRun().
                 * @tparam K Affinity key type.
                 * @tparam F Compute function type. Should implement
                 *  ComputeFunc<R> class.
                 * @tparam J Job type.
                 * @tparam T Task type.
                 * @param cacheName Cache name to use for affinity co-location.
                 * @param key Affinity key.
                 * @param func Function.
                 * @param opType Type of the operation.
                 * @return Future that can be used to access computation result
                 *  once it's ready.
                 */
                template<typename R, typename K, typename F, typename J, typename T>
                Future<R> PerformAffinityTask(const std::string& cacheName,
                    const K& key, const F& func, Operation::Type opType)
                {
                    enum { TYP_OBJ = 9 };

                    common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(mem.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    common::concurrent::SharedPointer<ComputeJobHolder> job(new J(func));

                    int64_t jobHandle = GetEnvironment().GetHandleRegistry().Allocate(job);

                    T* taskPtr = new T(jobHandle);
                    common::concurrent::SharedPointer<ComputeTaskHolder> task(taskPtr);

                    int64_t taskHandle = GetEnvironment().GetHandleRegistry().Allocate(task);

                    writer.WriteString(cacheName);
                    writer.WriteObject<K>(key);
                    writer.WriteObject<F>(func);
                    writer.WriteInt64(jobHandle);
                    writer.WriteInt64(taskHandle);
                    writer.WriteInt32(TYP_OBJ);

                    out.Synchronize();

                    IgniteError err;
                    jobject target = InStreamOutObject(opType, *mem.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    std::auto_ptr<common::Cancelable> cancelable(new CancelableImpl(GetEnvironmentPointer(), target));

                    common::Promise<R>& promise = taskPtr->GetPromise();
                    promise.SetCancelTarget(cancelable);

                    return promise.GetFuture();
                }

                IGNITE_NO_COPY_ASSIGNMENT(ComputeImpl);
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_IMPL
