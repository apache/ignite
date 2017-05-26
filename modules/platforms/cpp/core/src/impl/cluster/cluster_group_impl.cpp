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

#include "ignite/impl/cluster/cluster_group_impl.h"

using namespace ignite::jni::java;
using namespace ignite::impl::cluster;

namespace ignite
{
    namespace impl
    {
        namespace cluster
        {
            struct Command
            {
                enum Type
                {
                    FOR_SERVERS = 23
                };
            };

            ClusterGroupImpl::ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef) :
                InteropTarget(env, javaRef)
            {
                // No-op.
            }

            ClusterGroupImpl::~ClusterGroupImpl()
            {
                // No-op.
            }

            ClusterGroupImpl::SP_ClusterGroupImpl ClusterGroupImpl::ForServers(IgniteError& err)
            {
                JniErrorInfo jniErr;

                jobject res = InOpObject(Command::FOR_SERVERS, err);

                if (jniErr.code != java::IGNITE_JNI_ERR_SUCCESS)
                    return SP_ClusterGroupImpl();

                return FromTarget(res);
            }

            ClusterGroupImpl::SP_ClusterGroupImpl ClusterGroupImpl::FromTarget(jobject javaRef)
            {
                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), javaRef));
            }
        }
    }
}

