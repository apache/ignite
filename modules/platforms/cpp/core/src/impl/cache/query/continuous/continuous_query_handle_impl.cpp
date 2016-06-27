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
                    ContinuousQueryHandleImpl::ContinuousQueryHandleImpl(SP_IgniteEnvironment env, jobject javaRef) :
                        env(env),
                        javaRef(javaRef),
                        extracted(false)
                    {
                        // No-op.
                    }

                    ContinuousQueryHandleImpl::~ContinuousQueryHandleImpl()
                    {
                        JniContext::Release(javaRef);
                    }

                    QueryCursorImpl* ContinuousQueryHandleImpl::GetInitialQueryCursor(IgniteError& err)
                    {
                        if (extracted)
                        {
                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "GetInitialQueryCursor() can be called only once.");

                            return 0;
                        }

                        JniErrorInfo jniErr;

                        jobject res = env.Get()->Context()->ContinuousQueryGetInitialQueryCursor(javaRef, &jniErr);

                        IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                        if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                            return 0;

                        extracted = true;

                        return new QueryCursorImpl(env, res);
                    }
                }
            }
        }
    }
}
