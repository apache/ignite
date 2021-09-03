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
 * Declares ignite::compute::Compute class.
 */

#ifndef _IGNITE_COMPUTE_COMPUTE
#define _IGNITE_COMPUTE_COMPUTE

#include <ignite/common/common.h>

#include <ignite/ignite_error.h>
#include <ignite/future.h>
#include <ignite/compute/compute_func.h>

#include <ignite/impl/compute/compute_impl.h>

namespace ignite
{
    namespace compute
    {
        /**
         * Defines compute grid functionality for executing tasks and closures
         * over nodes in the ClusterGroup. Instance of Compute is obtained from
         * Ignite as follows:
         * @code{.cpp}
         * Ignite node = Ignition::Get();
         *
         * // Compute over all nodes in the cluster.
         * Compute c = node.GetCompute();
         * @endcode
         *
         * @par Load Balancing
         * In all cases other than <tt>Broadcast(...)</tt>, Ignite must select
         * a node for a computation to be executed. The node will be selected
         * based on the underlying \c LoadBalancingSpi, which by default
         * sequentially picks next available node from the underlying cluster
         * group. Other load balancing policies, such as \c random or
         * \c adaptive, can be configured as well by selecting a different
         * load balancing SPI in Ignite configuration.
         *
         * @par Fault Tolerance
         * Ignite guarantees that as long as there is at least one grid node
         * standing, every job will be executed. Jobs will automatically
         * failover to another node if a remote node crashed or has rejected
         * execution due to lack of resources. By default, in case of failover,
         * next load balanced node will be picked for job execution. Also jobs
         * will never be re-routed to the nodes they have failed on. This
         * behavior can be changed by configuring any of the existing or a
         * custom FailoverSpi in grid configuration.
         *
         * @par Computation SPIs
         * Note that regardless of which method is used for executing
         * computations, all relevant SPI implementations configured for this
         * compute instance will be used (i.e. failover, load balancing,
         * collision resolution, checkpoints, etc.).
         */
        class IGNITE_IMPORT_EXPORT Compute
        {
        public:
            /**
             * Constructor.
             *
             * Internal method. Should not be used by user.
             *
             * @param impl Implementation.
             */
            Compute(common::concurrent::SharedPointer<impl::compute::ComputeImpl> impl) :
                impl(impl)
            {
                // No-op.
            }

            /**
             * Executes given job on the node where data for
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
             * @return Computation result.
             * @throw IgniteError in case of error.
             */
            template<typename R, typename K, typename F>
            R AffinityCall(const std::string& cacheName, const K& key, const F& func)
            {
                return impl.Get()->AffinityCallAsync<R, K, F>(cacheName, key, func).GetValue();
            }

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
                return impl.Get()->AffinityCallAsync<R, K, F>(cacheName, key, func);
            }

            /**
             * Executes given job on the node where data for
             * provided affinity key is located (a.k.a. affinity co-location).
             *
             * @tparam K Affinity key type.
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param cacheName Cache names to use for affinity co-location.
             * @param key Affinity key.
             * @param action Compute action to call.
             * @throw IgniteError in case of error.
             */
            template<typename K, typename F>
            void AffinityRun(const std::string& cacheName, const K& key, const F& action)
            {
                return impl.Get()->AffinityRunAsync<K, F>(cacheName, key, action).GetValue();
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
                return impl.Get()->AffinityRunAsync<K, F>(cacheName, key, action);
            }

            /**
             * Calls provided ComputeFunc on a node within the underlying
             * cluster group.
             *
             * @tparam R Call return type. BinaryType should be specialized for
             *  the type if it is not primitive. Should not be void. For
             *  non-returning methods see Compute::Run().
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param func Compute function to call.
             * @return Computation result.
             * @throw IgniteError in case of error.
             */
            template<typename R, typename F>
            R Call(const F& func)
            {
                return impl.Get()->CallAsync<R, F>(func).GetValue();
            }

            /**
             * Asyncronuously calls provided ComputeFunc on a node within
             * the underlying cluster group.
             *
             * @tparam R Call return type. BinaryType should be specialized for
             *  the type if it is not primitive. Should not be void. For
             *  non-returning methods see Compute::Run().
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param func Compute function to call.
             * @return Future that can be used to access computation result once
             *  it's ready.
             * @throw IgniteError in case of error.
             */
            template<typename R, typename F>
            Future<R> CallAsync(const F& func)
            {
                return impl.Get()->CallAsync<R, F>(func);
            }

            /**
             * Runs provided ComputeFunc on a node within the underlying cluster
             * group.
             *
             * @tparam F Compute function type. Should implement ComputeFunc<void>
             *  class.
             * @param action Compute function to call.
             * @throw IgniteError in case of error.
             */
            template<typename F>
            void Run(const F& action)
            {
                return impl.Get()->RunAsync<F>(action).GetValue();
            }

            /**
             * Asyncronuously runs provided ComputeFunc on a node within the
             * underlying cluster group.
             *
             * @tparam F Compute function type. Should implement ComputeFunc<void>
             *  class.
             * @param action Compute function to call.
             * @return Future that can be used to wait for action to complete.
             * @throw IgniteError in case of error.
             */
            template<typename F>
            Future<void> RunAsync(const F& action)
            {
                return impl.Get()->RunAsync<F>(action);
            }

            /**
             * Broadcasts provided ComputeFunc to all nodes in the cluster group.
             *
             * @tparam R Function return type. BinaryType should be specialized
             *  for the type if it is not primitive.
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param func Compute function to call.
             * @return Vector containing computation results.
             * @throw IgniteError in case of error.
             */
            template<typename R, typename F>
            std::vector<R> Broadcast(const F& func)
            {
                return impl.Get()->BroadcastAsync<R, F>(func).GetValue();
            }

            /**
             * Broadcasts provided ComputeFunc to all nodes in the cluster group.
             *
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param func Compute function to call.
             * @throw IgniteError in case of error.
             */
            template<typename F>
            void Broadcast(const F& func)
            {
                impl.Get()->BroadcastAsync<F, false>(func).GetValue();
            }

            /**
             * Asyncronuously broadcasts provided ComputeFunc to all nodes in the
             * cluster group.
             *
             * @tparam R Function return type. BinaryType should be specialized
             *  for the type if it is not primitive.
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param func Compute function to call.
             * @return Future that can be used to access computation results once
             *  they are ready.
             * @throw IgniteError in case of error.
             */
            template<typename R, typename F>
            Future< std::vector<R> > BroadcastAsync(const F& func)
            {
                return impl.Get()->BroadcastAsync<R, F>(func);
            }

            /**
             * Asyncronuously broadcasts provided ComputeFunc to all nodes in the
             * cluster group.
             *
             * @tparam F Compute function type. Should implement ComputeFunc<R>
             *  class.
             * @param func Compute function to call.
             * @return Future that can be used to wait for action to complete.
             * @throw IgniteError in case of error.
             */
            template<typename F>
            Future<void> BroadcastAsync(const F& func)
            {
                return impl.Get()->BroadcastAsync<F, false>(func);
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
                return impl.Get()->ExecuteJavaTask<R, A>(taskName, taskArg);
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
                return impl.Get()->ExecuteJavaTask<R>(taskName);
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
                return impl.Get()->ExecuteJavaTaskAsync<R, A>(taskName, taskArg);
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
                return impl.Get()->ExecuteJavaTaskAsync<R>(taskName);
            }

        private:
            /** Implementation. */
            common::concurrent::SharedPointer<impl::compute::ComputeImpl> impl;
        };
    }
}

#endif //_IGNITE_COMPUTE_COMPUTE
