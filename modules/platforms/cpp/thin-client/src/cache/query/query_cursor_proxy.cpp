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

#include <ignite/impl/thin/cache/query/query_cursor_proxy.h>

#include "impl/cache/query/query_cursor_impl.h"

namespace
{
    using namespace ignite::common::concurrent;
    using namespace ignite::impl::thin::cache::query;

    QueryCursorImpl& GetQueryCursorImpl(SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<QueryCursorImpl*>(ptr.Get());
    }

    const QueryCursorImpl& GetQueryCursorImpl(const SharedPointer<void>& ptr)
    {
        return *reinterpret_cast<const QueryCursorImpl*>(ptr.Get());
    }
}

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                namespace query
                {
                    QueryCursorProxy::QueryCursorProxy(const common::concurrent::SharedPointer<void> &impl) :
                        impl(impl)
                    {
                        // No-op.
                    }

                    bool QueryCursorProxy::HasNext() const
                    {
                        return GetQueryCursorImpl(impl).HasNext();
                    }

                    void QueryCursorProxy::GetNext(Readable& entry)
                    {
                        GetQueryCursorImpl(impl).GetNext(entry);
                    }
                }
            }
        }
    }
}
