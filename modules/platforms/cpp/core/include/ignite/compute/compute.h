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
             * Calls provided ComputeFunc on a node within the underlying
             * cluster group.
             *
             * @tparam R Call return type. BinaryType should be specialized for
             *  the type if it is not primitive. Should not be void. For
             *  non-returning methods see Compute::Run().
             * @tparam F Compute function type. Should implement ComputeFunc
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
             * @tparam F Compute function type. Should implement ComputeFunc
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

        private:
            /** Implementation. */
            common::concurrent::SharedPointer<impl::compute::ComputeImpl> impl;
        };
    }
}

#endif //_IGNITE_COMPUTE_COMPUTE
