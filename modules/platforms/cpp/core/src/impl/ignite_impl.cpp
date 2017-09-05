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

#include "ignite/impl/ignite_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

using namespace ignite::binary;

namespace ignite
{    
    namespace impl
    {
        IgniteImpl::IgniteImpl(SharedPointer<IgniteEnvironment> env) :
            InteropTarget(env, static_cast<jobject>(env.Get()->GetProcessor()), true),
            env(env)
        {
            IgniteError err;

            txImpl = InternalGetTransactions(err);

            IgniteError::ThrowIfNeeded(err);

            prjImpl = InternalGetProjection(err);

            IgniteError::ThrowIfNeeded(err);
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

        IgniteImpl::SP_IgniteBindingImpl IgniteImpl::GetBinding()
        {
            return env.Get()->GetBinding();
        }

        IgniteImpl::SP_ComputeImpl IgniteImpl::GetCompute()
        {
            cluster::SP_ClusterGroupImpl serversCluster = prjImpl.Get()->ForServers();

            return serversCluster.Get()->GetCompute();
        }

        IgniteImpl::SP_TransactionsImpl IgniteImpl::InternalGetTransactions(IgniteError &err)
        {
            SP_TransactionsImpl res;

            jobject txJavaRef = InOpObject(ProcessorOp::GET_TRANSACTIONS, err);

            if (txJavaRef)
                res = SP_TransactionsImpl(new transactions::TransactionsImpl(env, txJavaRef));

            return res;
        }

        cluster::SP_ClusterGroupImpl IgniteImpl::InternalGetProjection(IgniteError& err)
        {
            cluster::SP_ClusterGroupImpl res;

            JniErrorInfo jniErr;

            jobject clusterGroupJavaRef = InOpObject(ProcessorOp::GET_CLUSTER_GROUP, err);

            if (clusterGroupJavaRef)
                res = cluster::SP_ClusterGroupImpl(new cluster::ClusterGroupImpl(env, clusterGroupJavaRef));

            return res;
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
