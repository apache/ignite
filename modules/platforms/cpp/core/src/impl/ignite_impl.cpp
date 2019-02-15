/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

        IgniteImpl::SP_IgniteBindingImpl IgniteImpl::GetBinding()
        {
            return env.Get()->GetBinding();
        }

        IgniteImpl::SP_ComputeImpl IgniteImpl::GetCompute()
        {
            cluster::SP_ClusterGroupImpl serversCluster = prjImpl.Get().Get()->ForServers();

            return serversCluster.Get()->GetCompute();
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
