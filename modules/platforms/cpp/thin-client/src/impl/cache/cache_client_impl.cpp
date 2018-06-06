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

#include <ignite/impl/thin/response_status.h>
#include <ignite/impl/thin/message.h>

#include <ignite/impl/thin/cache/cache_client_impl.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                void CacheClientImpl::Put(const Writable& key, const Writable& value)
                {
                    CachePutRequest req(id, binary, key, value);
                    Response rsp;

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }

                void CacheClientImpl::Get(const Writable& key, Readable& value)
                {
                    CacheGetRequest req(id, binary, key);
                    CacheGetResponse rsp(value);

                    router.Get()->SyncMessage(req, rsp);

                    if (rsp.GetStatus() != ResponseStatus::SUCCESS)
                        throw IgniteError(IgniteError::IGNITE_ERR_CACHE, rsp.GetError().c_str());
                }
            }
        }
    }
}

