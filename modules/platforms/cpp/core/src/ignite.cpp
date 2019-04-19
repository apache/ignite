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

    compute::Compute Ignite::GetCompute()
    {
        return compute::Compute(impl.Get()->GetCompute());
    }

    IgniteBinding Ignite::GetBinding()
    {
        return impl.Get()->GetBinding();
    }
}

