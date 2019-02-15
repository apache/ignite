/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
                    struct Command
                    {
                        enum Type
                        {
                            GET_INITIAL_QUERY = 0,

                            CLOSE = 1
                        };
                    };

                    ContinuousQueryHandleImpl::ContinuousQueryHandleImpl(SP_IgniteEnvironment env, int64_t handle, jobject javaRef) :
                        env(env),
                        handle(handle),
                        javaRef(javaRef),
                        mutex(),
                        extracted(false)
                    {
                        // No-op.
                    }

                    ContinuousQueryHandleImpl::~ContinuousQueryHandleImpl()
                    {
                        env.Get()->Context()->TargetInLongOutLong(javaRef, Command::CLOSE, 0);

                        JniContext::Release(javaRef);

                        env.Get()->GetHandleRegistry().Release(handle);
                    }

                    QueryCursorImpl* ContinuousQueryHandleImpl::GetInitialQueryCursor(IgniteError& err)
                    {
                        CsLockGuard guard(mutex);

                        if (extracted)
                        {
                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                                "GetInitialQueryCursor() can be called only once.");

                            return 0;
                        }

                        JniErrorInfo jniErr;

                        jobject res = env.Get()->Context()->TargetOutObject(javaRef, Command::GET_INITIAL_QUERY, &jniErr);

                        IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

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
