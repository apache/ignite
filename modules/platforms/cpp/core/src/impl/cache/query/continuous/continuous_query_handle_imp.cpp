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

#include <stdint.h>

#include "ignite/impl/cache/query/continuous/continuous_query_handle_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                namespace continuous
                {
                    ContinuousQueryHandleImpl::ContinuousQueryHandleImpl(SharedPointer<IgniteEnvironment> env, jobject javaRef) :
                        env(env),
                        javaRef(javaRef)
                    {
                        // No-op.
                    }

                    ContinuousQueryHandleImpl::~ContinuousQueryHandleImpl()
                    {
                        JniContext::Release(javaRef);
                    }
                }
            }
        }
    }
}
