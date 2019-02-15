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

#include "impl/data_router.h"

#include "impl/message.h"
#include "impl/remote_type_updater.h"

namespace ignite
{    
    namespace impl
    {
        namespace thin
        {
            namespace net
            {
                RemoteTypeUpdater::RemoteTypeUpdater(DataRouter& router):
                    router(router)
                {
                    // No-op.
                }

                RemoteTypeUpdater::~RemoteTypeUpdater()
                {
                    // No-op.
                }

                bool RemoteTypeUpdater::Update(const binary::Snap& snapshot, IgniteError& err)
                {
                    BinaryTypePutRequest req(snapshot);
                    Response rsp;

                    try
                    {
                        router.SyncMessageNoMetaUpdate(req, rsp);
                    }
                    catch(const IgniteError& e)
                    {
                        err = e;

                        return false;
                    }

                    return true;
                }

                binary::SPSnap RemoteTypeUpdater::GetMeta(int32_t typeId, IgniteError& err)
                {
                    binary::SPSnap snap;

                    BinaryTypeGetRequest req(typeId);
                    BinaryTypeGetResponse rsp(snap);

                    try
                    {
                        router.SyncMessageNoMetaUpdate(req, rsp);
                    }
                    catch(const IgniteError& e)
                    {
                        err = e;

                        return binary::SPSnap();
                    }

                    return snap;
                }
            }
        }
    }    
}
