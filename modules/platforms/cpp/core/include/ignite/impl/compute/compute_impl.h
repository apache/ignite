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

#include <ignite/cluster/cluster_node.h>

#include <ignite/impl/interop/interop_target.h>
#include <ignite/impl/cluster/cluster_group_impl.h>
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
                 * @param clusterGroup Cluster group for the compute.
                 */
                ComputeImpl(common::concurrent::SharedPointer<IgniteEnvironment> env,
                            cluster::SP_ClusterGroupImpl clusterGroup);

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
                 * Executes given Java task on the grid projection. If task for given name has not been deployed yet,
                 * then 'taskName' will be used as task class name to auto-deploy the task.
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
                    return PerformJavaTask<R, A>(taskName, &taskArg);
                }

                /**
                 * Executes given Java task on the grid projection. If task for given name has not been deployed yet,
                 * then 'taskName' will be used as task class name to auto-deploy the task.
                 *
                 * @param taskName Java task name.
                 * @return Task result of type @c R.
                 *
                 * @tparam R Type of task result.
                 */
                template<typename R>
                R ExecuteJavaTask(const std::string& taskName)
                {
                    return PerformJavaTask<R, int>(taskName, 0);
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
                    return PerformJavaTaskAsync<R, A>(taskName, &taskArg);
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
                    return PerformJavaTaskAsync<R, int>(taskName, 0);
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ComputeImpl);

                struct FutureType
                {
                    enum Type
                    {
                        F_BYTE = 1,
                        F_BOOL = 2,
                        F_SHORT = 3,
                        F_CHAR = 4,
                        F_INT = 5,
                        F_FLOAT = 6,
                        F_LONG = 7,
                        F_DOUBLE = 8,
                        F_OBJECT = 9,
                    };
                };

                template<typename T> struct FutureTypeForType { static const int32_t value = FutureType::F_OBJECT; };

                /**
                 * @return True if projection for the compute contains predicate.
                 */
                bool ProjectionContainsPredicate() const;

                /**
                 * @return Nodes for the compute.
                 */
                std::vector<ignite::cluster::ClusterNode> GetNodes();

                /**
                 * Write Java task using provided writer. If task for given name has not been deployed yet,
                 * then 'taskName' will be used as task class name to auto-deploy the task.
                 *
                 * @param taskName Java task name.
                 * @param taskArg Argument of task execution of type A.
                 * @param writer Binary writer.
                 * @return Task result of type @c R.
                 *
                 * @tparam R Type of task result.
                 * @tparam A Type of task argument.
                 */
                template<typename A>
                void WriteJavaTask(const std::string& taskName, const A* arg, binary::BinaryWriterImpl& writer) {
                    writer.WriteString(taskName);

                    // Keep binary flag
                    writer.WriteBool(false);
                    if (arg)
                        writer.WriteObject<A>(*arg);
                    else
                        writer.WriteNull();

                    if (!ProjectionContainsPredicate())
                        writer.WriteBool(false);
                    else
                    {
                        typedef std::vector<ignite::cluster::ClusterNode> ClusterNodes;
                        ClusterNodes nodes = GetNodes();

                        writer.WriteBool(true);
                        writer.WriteInt32(static_cast<int32_t>(nodes.size()));
                        for (ClusterNodes::iterator it = nodes.begin(); it != nodes.end(); ++it)
                            writer.WriteGuid(it->GetId());
                    }
                }

                /**
                 * Executes given Java task on the grid projection. If task for given name has not been deployed yet,
                 * then 'taskName' will be used as task class name to auto-deploy the task.
                 *
                 * @param taskName Java task name.
                 * @param taskArg Argument of task execution of type A.
                 * @return Task result of type @c R.
                 *
                 * @tparam R Type of task result.
                 * @tparam A Type of task argument.
                 */
                template<typename R, typename A>
                R PerformJavaTask(const std::string& taskName, const A* arg)
                {
                    using namespace common::concurrent;

                    SharedPointer<interop::InteropMemory> memIn = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(memIn.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    WriteJavaTask(taskName, arg, writer);

                    out.Synchronize();

                    SharedPointer<interop::InteropMemory> memOut = GetEnvironment().AllocateMemory();

                    IgniteError err;
                    InStreamOutStream(Operation::EXEC, *memIn.Get(), *memOut.Get(), err);
                    IgniteError::ThrowIfNeeded(err);

                    interop::InteropInputStream inStream(memOut.Get());
                    binary::BinaryReaderImpl reader(&inStream);

                    return reader.ReadObject<R>();
                }

                /**
                 * Executes given Java task on the grid projection. If task for given name has not been deployed yet,
                 * then 'taskName' will be used as task class name to auto-deploy the task.
                 *
                 * @param taskName Java task name.
                 * @param arg Argument of task execution of type A.
                 * @return Task result of type @c R.
                 *
                 * @tparam R Type of task result.
                 * @tparam A Type of task argument.
                 */
                template<typename R, typename A>
                Future<R> PerformJavaTaskAsync(const std::string& taskName, const A* arg)
                {
                    typedef JavaComputeTaskHolder<R> TaskHolder;
                    common::concurrent::SharedPointer<TaskHolder> task(new TaskHolder());
                    int64_t taskHandle = GetEnvironment().GetHandleRegistry().Allocate(task);

                    common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropOutputStream out(mem.Get());
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());

                    WriteJavaTask(taskName, arg, writer);

                    writer.WriteInt64(taskHandle);
                    writer.WriteInt32(FutureTypeForType<R>::value);

                    out.Synchronize();

                    IgniteError err;
                    jobject target = InStreamOutObject(Operation::EXEC_ASYNC, *mem.Get(), err);
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

                /** Cluster group */
                cluster::SP_ClusterGroupImpl clusterGroup;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<int8_t> {
                static const int32_t value = FutureType::F_BYTE;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<bool> {
                static const int32_t value = FutureType::F_BOOL;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<int16_t> {
                static const int32_t value = FutureType::F_SHORT;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<uint16_t> {
                static const int32_t value = FutureType::F_CHAR;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<int32_t> {
                static const int32_t value = FutureType::F_INT;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<int64_t> {
                static const int32_t value = FutureType::F_LONG;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<float> {
                static const int32_t value = FutureType::F_FLOAT;
            };

            template<> struct IGNITE_IMPORT_EXPORT ComputeImpl::FutureTypeForType<double> {
                static const int32_t value = FutureType::F_DOUBLE;
            };
        }
    }
}

#endif //_IGNITE_IMPL_COMPUTE_COMPUTE_IMPL
