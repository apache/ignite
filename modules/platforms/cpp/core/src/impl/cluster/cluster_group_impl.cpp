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
                    FOR_SERVERS = 23,

                    SET_ACTIVE = 28,

                    IS_ACTIVE = 29
                };
            };

            ClusterGroupImpl::ClusterGroupImpl(SP_IgniteEnvironment env, jobject javaRef) :
                InteropTarget(env, javaRef)
            {
                computeImpl = InternalGetCompute();
            }

            ClusterGroupImpl::~ClusterGroupImpl()
            {
                // No-op.
            }

            SP_ClusterGroupImpl ClusterGroupImpl::ForServers()
            {
                IgniteError err;

                jobject res = InOpObject(Command::FOR_SERVERS, err);

                IgniteError::ThrowIfNeeded(err);

                return FromTarget(res);
            }

            ClusterGroupImpl::SP_ComputeImpl ClusterGroupImpl::GetCompute()
            {
                return computeImpl;
            }

            bool ClusterGroupImpl::IsActive()
            {
                IgniteError err;

                int64_t res = OutInOpLong(Command::IS_ACTIVE, 0, err);

                IgniteError::ThrowIfNeeded(err);

                return res == 1;
            }

            void ClusterGroupImpl::SetActive(bool active)
            {
                IgniteError err;

                OutInOpLong(Command::SET_ACTIVE, active ? 1 : 0, err);

                IgniteError::ThrowIfNeeded(err);
            }

            SP_ClusterGroupImpl ClusterGroupImpl::FromTarget(jobject javaRef)
            {
                return SP_ClusterGroupImpl(new ClusterGroupImpl(GetEnvironmentPointer(), javaRef));
            }

            ClusterGroupImpl::SP_ComputeImpl ClusterGroupImpl::InternalGetCompute()
            {
                jobject computeProc = GetEnvironment().GetProcessorCompute(GetTarget());

                return SP_ComputeImpl(new compute::ComputeImpl(GetEnvironmentPointer(), computeProc));
            }
        }
    }
}

