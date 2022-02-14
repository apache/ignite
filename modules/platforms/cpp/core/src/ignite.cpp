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

#include <ignite/jni/java.h>

#include "ignite/impl/ignite_impl.h"
#include "ignite/ignite.h"

using namespace ignite::common::concurrent;
using namespace ignite::impl;

namespace ignite
{
    Ignite::Ignite() : impl(SharedPointer<IgniteImpl>())
    {
        // No-op.
    }

    Ignite::Ignite(IgniteImpl* impl) : impl(SharedPointer<IgniteImpl>(impl))
    {
        // No-op.
    }

    const char* Ignite::GetName() const
    {
        return impl.Get()->GetName();
    }

    const IgniteConfiguration& Ignite::GetConfiguration() const
    {
        return impl.Get()->GetConfiguration();
    }

    bool Ignite::IsActive()
    {
        return impl.Get()->IsActive();
    }

    void Ignite::SetActive(bool active)
    {
        impl.Get()->SetActive(active);
    }

    transactions::Transactions Ignite::GetTransactions()
    {
        using ignite::common::concurrent::SharedPointer;
        using ignite::impl::transactions::TransactionsImpl;

        SharedPointer<TransactionsImpl> txImpl = impl.Get()->GetTransactions();

        return transactions::Transactions(txImpl);
    }

    cluster::IgniteCluster Ignite::GetCluster()
    {
        return cluster::IgniteCluster(impl.Get()->GetCluster());
    }

    compute::Compute Ignite::GetCompute()
    {
        return compute::Compute(impl.Get()->GetCompute());
    }

    compute::Compute Ignite::GetCompute(cluster::ClusterGroup grp)
    {
        return compute::Compute(impl.Get()->GetCompute(grp));
    }

    IgniteBinding Ignite::GetBinding()
    {
        return impl.Get()->GetBinding();
    }
}

