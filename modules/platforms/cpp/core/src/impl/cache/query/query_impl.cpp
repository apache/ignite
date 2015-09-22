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

#include "ignite/impl/cache/query/query_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::common::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::portable;

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            namespace query
            {
                /** Operation: get all entries. */
                const int32_t OP_GET_ALL = 1;

                /** Operation: get single entry. */
                const int32_t OP_GET_SINGLE = 3;

                QueryCursorImpl::QueryCursorImpl(SharedPointer<IgniteEnvironment> env, jobject javaRef) :
                    env(env), javaRef(javaRef), iterCalled(false), getAllCalled(false), hasNext(false)
                {
                    // No-op.
                }

                QueryCursorImpl::~QueryCursorImpl()
                {
                    // 1. Close the cursor.
                    env.Get()->Context()->QueryCursorClose(javaRef);

                    // 2. Release Java reference.
                    JniContext::Release(javaRef);
                }

                bool QueryCursorImpl::HasNext(IgniteError* err)
                {
                    // Check whether GetAll() was called earlier.
                    if (getAllCalled) 
                    {
                        *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                            "Cannot use HasNext() method because GetAll() was called.");

                        return false;
                    }

                    // Create iterator in Java if needed.
                    if (!CreateIteratorIfNeeded(err))
                        return false;
                    
                    return hasNext;
                }

                void QueryCursorImpl::GetNext(OutputOperation& op, IgniteError* err)
                {
                    // Check whether GetAll() was called earlier.
                    if (getAllCalled) 
                    {
                        *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                            "Cannot use GetNext() method because GetAll() was called.");

                        return;
                    }

                    // Create iterator in Java if needed.
                    if (!CreateIteratorIfNeeded(err))
                        return;

                    if (hasNext)
                    {
                        JniErrorInfo jniErr;

                        SharedPointer<InteropMemory> inMem = env.Get()->AllocateMemory();

                        env.Get()->Context()->TargetOutStream(javaRef, OP_GET_SINGLE, inMem.Get()->PointerLong(), &jniErr);

                        IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                        if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        {
                            InteropInputStream in(inMem.Get());

                            portable::PortableReaderImpl reader(&in);

                            op.ProcessOutput(reader);

                            hasNext = IteratorHasNext(err);
                        }
                    }
                    else
                    {
                        // Ensure we do not overwrite possible previous error.
                        if (err->GetCode() == IgniteError::IGNITE_SUCCESS)
                            *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, "No more elements available.");
                    }
                }

                void QueryCursorImpl::GetAll(OutputOperation& op, IgniteError* err)
                {
                    // Check whether any of iterator methods were called.
                    if (iterCalled)
                    {
                        *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Cannot use GetAll() method because an iteration method was called.");

                        return;
                    }

                    // Check whether GetAll was called before.
                    if (getAllCalled)
                    {
                        *err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Cannot use GetNext() method because GetAll() was called.");

                        return;
                    }

                    // Get data.
                    JniErrorInfo jniErr;

                    SharedPointer<InteropMemory> inMem = env.Get()->AllocateMemory();

                    env.Get()->Context()->TargetOutStream(javaRef, OP_GET_ALL, inMem.Get()->PointerLong(), &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                    {
                        getAllCalled = true;

                        InteropInputStream in(inMem.Get());

                        portable::PortableReaderImpl reader(&in);

                        op.ProcessOutput(reader);
                    }
                }

                bool QueryCursorImpl::CreateIteratorIfNeeded(IgniteError* err)
                {
                    if (!iterCalled)
                    {
                        JniErrorInfo jniErr;

                        env.Get()->Context()->QueryCursorIterator(javaRef, &jniErr);

                        IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                        if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        {
                            iterCalled = true;

                            hasNext = IteratorHasNext(err);
                        }
                        else
                            return false;
                    }
                    
                    return true;
                }

                bool QueryCursorImpl::IteratorHasNext(IgniteError* err)
                {
                    JniErrorInfo jniErr;

                    bool res = env.Get()->Context()->QueryCursorIteratorHasNext(javaRef, &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    return jniErr.code == IGNITE_JNI_ERR_SUCCESS && res;
                }
            }
        }
    }
}