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

#ifndef _IGNITE_IMPL_IGNITE_IMPL
#define _IGNITE_IMPL_IGNITE_IMPL

#include <ignite/jni/java.h>
#include <ignite/common/utils.h>
#include <ignite/common/concurrent.h>
#include <ignite/common/lazy.h>
#include <ignite/cluster/cluster_group.h>

#include <ignite/impl/ignite_environment.h>
#include <ignite/impl/cache/cache_impl.h>
#include <ignite/impl/transactions/transactions_impl.h>
#include <ignite/impl/cluster/cluster_group_impl.h>
#include <ignite/impl/cluster/ignite_cluster_impl.h>
#include <ignite/impl/cache/cache_affinity_impl.h>
#include <ignite/impl/compute/compute_impl.h>
#include <ignite/impl/cluster/cluster_group_impl.h>

namespace ignite
{
    namespace impl
    {
        /**
         * Ignite implementation.
         */
        class IGNITE_FRIEND_EXPORT IgniteImpl : private interop::InteropTarget
        {
            typedef common::concurrent::SharedPointer<transactions::TransactionsImpl> SP_TransactionsImpl;
            typedef common::concurrent::SharedPointer<cluster::IgniteClusterImpl> SP_IgniteClusterImpl;
            typedef common::concurrent::SharedPointer<compute::ComputeImpl> SP_ComputeImpl;
            typedef common::concurrent::SharedPointer<IgniteBindingImpl> SP_IgniteBindingImpl;
            typedef common::concurrent::SharedPointer<cache::CacheAffinityImpl> SP_CacheAffinityImpl;
        public:
            /**
             * Constructor used to create new instance.
             *
             * @param env Environment.
             */
            IgniteImpl(SP_IgniteEnvironment env);

            /**
             * Get affinity service to provide information about data partitioning and distribution.
             *
             * @param name Cache name.
             * @param err Error.
             * @return Pointer to cache affinity implementation.
             */
            SP_CacheAffinityImpl GetAffinity(const std::string& cacheName, IgniteError& err);

            /**
             * Get name of the Ignite.
             *
             * @return Name.
             */
            const char* GetName() const;

            /**
             * Get node configuration.
             *
             * @return Node configuration.
             */
            const IgniteConfiguration& GetConfiguration() const;

            /**
             * Get JNI context associated with this instance.
             *
             * @return JNI context for this instance.
             */
            jni::java::JniContext* GetContext();

            /**
             * Get cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            cache::CacheImpl* GetCache(const char* name, IgniteError& err);

            /**
             * Get or create cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            cache::CacheImpl* GetOrCreateCache(const char* name, IgniteError& err);

            /**
             * Create cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            cache::CacheImpl* CreateCache(const char* name, IgniteError& err);

            /**
             * Get ignite binding.
             *
             * @return IgniteBinding class instance.
             */
            SP_IgniteBindingImpl GetBinding();

            /**
             * Get instance of the implementation from the proxy class.
             * Internal method. Should not be used by user.
             *
             * @param proxy Proxy instance containing IgniteImpl.
             * @return IgniteImpl instance associated with the proxy or null-pointer.
             */
            template<typename T>
            static IgniteImpl* GetFromProxy(T& proxy)
            {
                return proxy.impl.Get();
            }

            /**
             * Get environment.
             * Internal method. Should not be used by user.
             *
             * @return Environment pointer.
             */
            IgniteEnvironment* GetEnvironment()
            {
                return env.Get();
            }

            /**
             * Get transactions.
             *
             * @return TransactionsImpl instance.
             */
            SP_TransactionsImpl GetTransactions()
            {
                return txImpl.Get();
            }

            /**
             * Get projection.
             *
             * @return ClusterGroupImpl instance.
             */
            cluster::SP_ClusterGroupImpl GetProjection()
            {
                return prjImpl.Get();
            }

            /**
             * Get cluster.
             *
             * @return IgniteClusterImpl instance.
             */
            SP_IgniteClusterImpl GetCluster();

            /**
             * Gets compute instance over all cluster nodes started in server mode.
             *
             * @return ComputeImpl instance.
             */
            SP_ComputeImpl GetCompute();

            /**
             * Gets compute instance over the specified cluster group. All operations
             * on the returned compute instance will only include nodes from
             * this cluster group.
             *
             * @param grp Specified cluster group instance.
             * @return ComputeImpl instance.
             */
            SP_ComputeImpl GetCompute(ignite::cluster::ClusterGroup grp);

            /**
             * Check if the Ignite grid is active.
             *
             * @return True if grid is active and false otherwise.
             */
            bool IsActive()
            {
                return prjImpl.Get().Get()->IsActive();
            }

            /**
             * Change Ignite grid state to active or inactive.
             *
             * @param active If true start activation process. If false start
             *    deactivation process.
             */
            void SetActive(bool active)
            {
                prjImpl.Get().Get()->SetActive(active);
            }

            /**
             * Disable write-ahead logging for specified cache.
             *
             * @param cacheName Cache name.
             */
            void DisableWal(std::string cacheName);

            /**
             * Enable write-ahead logging for specified cache.
             *
             * @param cacheName Cache name.
             */
            void EnableWal(std::string cacheName);

            /**
             * Check if write - ahead logging is enabled for specified cache.
             *
             * @param cacheName Cache name.
             *
             * @return True if enabled.
             */
            bool IsWalEnabled(std::string cacheName);

            /**
             * Set baseline topology constructed from the cluster topology of the given version.
             * The method succeeds only if the cluster topology has not changed.
             *
             * @param topVer Topology version.
             */
            void SetBaselineTopologyVersion(int64_t topVer);

            /**
             * Set transaction timeout on partition map exchange.
             *
             * @param timeout Timeout in milliseconds.
             */
            void SetTxTimeoutOnPartitionMapExchange(int64_t timeout);

        private:
            /**
             * Get transactions internal call.
             *
             * @return TransactionsImpl instance.
             */
            transactions::TransactionsImpl* InternalGetTransactions();

            /**
             * Get current projection internal call.
             *
             * @return ClusterGroupImpl instance.
             */
            cluster::ClusterGroupImpl* InternalGetProjection();

            /** Environment. */
            SP_IgniteEnvironment env;

            /** Transactions implementaion. */
            common::Lazy<transactions::TransactionsImpl> txImpl;

            /** Projection implementation. */
            common::Lazy<cluster::ClusterGroupImpl> prjImpl;

            IGNITE_NO_COPY_ASSIGNMENT(IgniteImpl);

            /**
            * Get or create cache.
            *
            * @param name Cache name.
            * @param err Error.
            * @param op Operation code.
            */
            cache::CacheImpl* GetOrCreateCache(const char* name, IgniteError& err, int32_t op);
        };
    }
}

#endif //_IGNITE_IMPL_IGNITE_IMPL
