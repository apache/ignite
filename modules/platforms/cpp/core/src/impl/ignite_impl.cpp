/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ignite/impl/ignite_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::cluster;
using namespace ignite::jni::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::impl::cache;

using namespace ignite::binary;

namespace ignite
{
    namespace impl
    {
        /*
         * PlatformProcessor op codes.
         */
        struct ProcessorOp
        {
            enum Type
            {
                GET_CACHE = 1,
                CREATE_CACHE = 2,
                GET_OR_CREATE_CACHE = 3,
                GET_TRANSACTIONS = 9,
                GET_CLUSTER_GROUP = 10,
                SET_BASELINE_TOPOLOGY_VERSION = 24,
                WAL_DISABLE = 27,
                WAL_ENABLE = 28,
                WAL_IS_ENABLED = 29,
                SET_TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE = 30,
            };
        };

        IgniteImpl::IgniteImpl(SharedPointer<IgniteEnvironment> env) :
            InteropTarget(env, static_cast<jobject>(env.Get()->GetProcessor()), true),
            env(env),
            txImpl(),
            prjImpl()
        {
            txImpl.Init(common::Bind(this, &IgniteImpl::InternalGetTransactions));
            prjImpl.Init(common::Bind(this, &IgniteImpl::InternalGetProjection));
        }

        const char* IgniteImpl::GetName() const
        {
            return env.Get()->InstanceName();
        }

        const IgniteConfiguration& IgniteImpl::GetConfiguration() const
        {
            return env.Get()->GetConfiguration();
        }

        JniContext* IgniteImpl::GetContext()
        {
            return env.Get()->Context();
        }

        CacheImpl* IgniteImpl::GetCache(const char* name, IgniteError& err)
        {
            return GetOrCreateCache(name, err, ProcessorOp::GET_CACHE);
        }

        CacheImpl* IgniteImpl::GetOrCreateCache(const char* name, IgniteError& err)
        {
            return GetOrCreateCache(name, err, ProcessorOp::GET_OR_CREATE_CACHE);
        }

        CacheImpl* IgniteImpl::CreateCache(const char* name, IgniteError& err)
        {
            return GetOrCreateCache(name, err, ProcessorOp::CREATE_CACHE);
        }

        IgniteImpl::SP_IgniteBindingImpl IgniteImpl::GetBinding()
        {
            return env.Get()->GetBinding();
        }

        IgniteImpl::SP_IgniteClusterImpl IgniteImpl::GetCluster()
        {
            return IgniteImpl::SP_IgniteClusterImpl(new cluster::IgniteClusterImpl(this->GetProjection()));
        }

        IgniteImpl::SP_ComputeImpl IgniteImpl::GetCompute()
        {
            cluster::SP_ClusterGroupImpl serversCluster = prjImpl.Get().Get()->ForServers();

            return serversCluster.Get()->GetCompute();
        }

        IgniteImpl::SP_ComputeImpl IgniteImpl::GetCompute(ClusterGroup grp)
        {
            return this->GetProjection().Get()->GetCompute(grp);
        }

        void IgniteImpl::DisableWal(std::string cacheName)
        {
            IgniteError err;
            In1Operation<std::string> inOp(cacheName);

            InteropTarget::OutOp(ProcessorOp::WAL_DISABLE, inOp, err);

            IgniteError::ThrowIfNeeded(err);
        }

        void IgniteImpl::EnableWal(std::string cacheName)
        {
            IgniteError err;
            In1Operation<std::string> inOp(cacheName);

            InteropTarget::OutOp(ProcessorOp::WAL_ENABLE, inOp, err);

            IgniteError::ThrowIfNeeded(err);
        }

        bool IgniteImpl::IsWalEnabled(std::string cacheName)
        {
            IgniteError err;
            In1Operation<std::string> inOp(cacheName);

            bool ret = InteropTarget::OutOp(ProcessorOp::WAL_IS_ENABLED, inOp, err);

            IgniteError::ThrowIfNeeded(err);

            return ret;
        }

        void IgniteImpl::SetBaselineTopologyVersion(long topVer)
        {
            IgniteError err;

            OutInOpLong(ProcessorOp::SET_BASELINE_TOPOLOGY_VERSION, topVer, err);

            IgniteError::ThrowIfNeeded(err);
        }

        void IgniteImpl::SetTxTimeoutOnPartitionMapExchange(long timeout)
        {
            IgniteError err;

            if (timeout < 0) {
                const char* msg = "Impossible to set negative timeout";
                throw IgniteError(IgniteError::IGNITE_ERR_ILLEGAL_ARGUMENT, msg);
            }

            In1Operation<int64_t> inOp(timeout);

            OutOp(ProcessorOp::SET_TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE, inOp, err);

            IgniteError::ThrowIfNeeded(err);
        }

        transactions::TransactionsImpl* IgniteImpl::InternalGetTransactions()
        {
            IgniteError err;

            jobject txJavaRef = InOpObject(ProcessorOp::GET_TRANSACTIONS, err);

            IgniteError::ThrowIfNeeded(err);

            if (!txJavaRef)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not get Transactions instance.");

            return new transactions::TransactionsImpl(env, txJavaRef);
        }

        cluster::ClusterGroupImpl* IgniteImpl::InternalGetProjection()
        {
            IgniteError err;

            jobject clusterGroupJavaRef = InOpObject(ProcessorOp::GET_CLUSTER_GROUP, err);

            IgniteError::ThrowIfNeeded(err);

            if (!clusterGroupJavaRef)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Can not get ClusterGroup instance.");

            return new cluster::ClusterGroupImpl(env, clusterGroupJavaRef);
        }

        cache::CacheImpl* IgniteImpl::GetOrCreateCache(const char* name, IgniteError& err, int32_t op)
        {
            SharedPointer<InteropMemory> mem = env.Get()->AllocateMemory();
            InteropMemory* mem0 = mem.Get();
            InteropOutputStream out(mem0);
            BinaryWriterImpl writer(&out, env.Get()->GetTypeManager());
            BinaryRawWriter rawWriter(&writer);

            rawWriter.WriteString(name);

            out.Synchronize();

            jobject cacheJavaRef = InStreamOutObject(op, *mem0, err);

            if (!cacheJavaRef)
            {
                return NULL;
            }

            char* name0 = common::CopyChars(name);

            return new cache::CacheImpl(name0, env, cacheJavaRef);
        }
    }
}
