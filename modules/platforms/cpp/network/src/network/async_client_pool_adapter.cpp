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

#include "network/async_client_pool_adapter.h"
#include "network/error_handling_filter.h"

namespace ignite
{
    namespace network
    {
        AsyncClientPoolAdapter::AsyncClientPoolAdapter(
            const std::vector<SP_DataFilter> &filters0,
            const SP_AsyncClientPool& pool0
        ) :
            filters(filters0),
            pool(pool0),
            sink(pool.Get()),
            handler(0)
        {
            filters.insert(filters.begin(), SP_DataFilter(new ErrorHandlingFilter()));

            for (std::vector<SP_DataFilter>::iterator it = filters.begin(); it != filters.end(); ++it)
            {
                it->Get()->SetSink(sink);
                sink = it->Get();
            }
        }

        void AsyncClientPoolAdapter::Start(const std::vector<TcpRange>& addrs, uint32_t connLimit)
        {
            pool.Get()->Start(addrs, connLimit);
        }

        void AsyncClientPoolAdapter::Stop()
        {
            pool.Get()->Stop();
        }

        void AsyncClientPoolAdapter::SetHandler(AsyncHandler* handler0)
        {
            handler = handler0;
            for (std::vector<SP_DataFilter>::reverse_iterator it = filters.rbegin(); it != filters.rend(); ++it)
            {
                it->Get()->SetHandler(handler);
                handler = it->Get();
            }

            pool.Get()->SetHandler(handler);
        }

        bool AsyncClientPoolAdapter::Send(uint64_t id, const DataBuffer& data)
        {
            return sink->Send(id, data);
        }

        void AsyncClientPoolAdapter::Close(uint64_t id, const IgniteError* err)
        {
            sink->Close(id, err);
        }
    }
}
