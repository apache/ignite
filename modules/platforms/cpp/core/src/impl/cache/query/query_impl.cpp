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

#include "ignite/impl/cache/query/query_impl.h"
#include "ignite/impl/cache/query/query_fields_row_impl.h"

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
                /** Operation: get all entries. */
                const int32_t OP_GET_ALL = 1;

                /** Operation: get multiple entries. */
                const int32_t OP_GET_BATCH = 2;

                /** Operation: get single entry. */
                const int32_t OP_GET_SINGLE = 3;

                /** Operation: start iterator. */
                const int32_t OP_ITERATOR = 4;

                /** Operation: close iterator. */
                const int32_t OP_ITERATOR_CLOSE = 5;

                /** Operation: close iterator. */
                const int32_t OP_ITERATOR_HAS_NEXT = 6;

                QueryCursorImpl::QueryCursorImpl(SharedPointer<IgniteEnvironment> env, jobject javaRef) :
                    env(env),
                    javaRef(javaRef),
                    batch(0),
                    endReached(false),
                    iterCalled(false),
                    getAllCalled(false)
                {
                    // No-op.
                }

                QueryCursorImpl::~QueryCursorImpl()
                {
                    // 1. Releasing memory.
                    delete batch;

                    // 2. Close the cursor.
                    env.Get()->Context()->TargetInLongOutLong(javaRef, OP_ITERATOR_CLOSE, 0);

                    // 3. Release Java reference.
                    JniContext::Release(javaRef);
                }

                bool QueryCursorImpl::HasNext(IgniteError& err)
                {
                    // Check whether GetAll() was called earlier.
                    if (getAllCalled) 
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                            "Cannot use HasNext() method because GetAll() was called.");

                        return false;
                    }

                    // Create iterator in Java if needed.
                    if (!CreateIteratorIfNeeded(err))
                        return false;

                    // Get next results batch if the end in the current batch
                    // has been reached.
                    if (!GetNextBatchIfNeeded(err))
                        return false;

                    return !endReached;
                }

                void QueryCursorImpl::GetNext(OutputOperation& op, IgniteError& err)
                {
                    // Check whether GetAll() was called earlier.
                    if (getAllCalled) 
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, 
                            "Cannot use GetNext() method because GetAll() was called.");

                        return;
                    }

                    // Create iterator in Java if needed.
                    if (!CreateIteratorIfNeeded(err))
                        return;

                    // Get next results batch if the end in the current batch
                    // has been reached.
                    if (!GetNextBatchIfNeeded(err))
                        return;

                    if (endReached)
                    {
                        // Ensure we do not overwrite possible previous error.
                        if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, "No more elements available.");

                        return;
                    }

                    batch->GetNext(op);
                }

                QueryFieldsRowImpl* QueryCursorImpl::GetNextRow(IgniteError& err)
                {
                    // Create iterator in Java if needed.
                    if (!CreateIteratorIfNeeded(err))
                        return 0;

                    // Get next results batch if the end in the current batch
                    // has been reached.
                    if (!GetNextBatchIfNeeded(err))
                        return 0;

                    if (endReached)
                    {
                        // Ensure we do not overwrite possible previous error.
                        if (err.GetCode() == IgniteError::IGNITE_SUCCESS)
                            err = IgniteError(IgniteError::IGNITE_ERR_GENERIC, "No more elements available.");

                        return 0;
                    }

                    return batch->GetNextRow();
                }

                void QueryCursorImpl::GetAll(OutputOperation& op, IgniteError& err)
                {
                    // Check whether any of iterator methods were called.
                    if (iterCalled)
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Cannot use GetAll() method because an iteration method was called.");

                        return;
                    }

                    // Check whether GetAll was called before.
                    if (getAllCalled)
                    {
                        err = IgniteError(IgniteError::IGNITE_ERR_GENERIC,
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

                        BinaryReaderImpl reader(&in);

                        op.ProcessOutput(reader);
                    }
                }

                void QueryCursorImpl::GetAll(OutputOperation& op)
                {
                    // Check whether any of iterator methods were called.
                    if (iterCalled)
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Cannot use GetAll() method because an iteration method was called.");
                    }

                    // Check whether GetAll was called before.
                    if (getAllCalled)
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Cannot use GetNext() method because GetAll() was called.");
                    }

                    // Get data.
                    JniErrorInfo jniErr;

                    SharedPointer<InteropMemory> inMem = env.Get()->AllocateMemory();

                    env.Get()->Context()->TargetOutStream(javaRef, OP_GET_ALL, inMem.Get()->PointerLong(), &jniErr);

                    IgniteError err;
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    IgniteError::ThrowIfNeeded(err);

                    getAllCalled = true;

                    InteropInputStream in(inMem.Get());

                    BinaryReaderImpl reader(&in);

                    op.ProcessOutput(reader);
                }

                bool QueryCursorImpl::CreateIteratorIfNeeded(IgniteError& err)
                {
                    if (iterCalled)
                        return true;

                    JniErrorInfo jniErr;

                    env.Get()->Context()->TargetInLongOutLong(javaRef, OP_ITERATOR, 0, &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        iterCalled = true;

                    return iterCalled;
                }

                bool QueryCursorImpl::GetNextBatchIfNeeded(IgniteError& err)
                {
                    assert(iterCalled);

                    if (endReached || (batch && batch->Left() > 0))
                        return true;

                    endReached = !IteratorHasNext(err);

                    if (endReached)
                        return true;

                    JniErrorInfo jniErr;

                    SharedPointer<InteropMemory> inMem = env.Get()->AllocateMemory();

                    env.Get()->Context()->TargetOutStream(
                        javaRef, OP_GET_BATCH, inMem.Get()->PointerLong(), &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code != IGNITE_JNI_ERR_SUCCESS)
                        return false;

                    delete batch;

                    // Needed for exception safety.
                    batch = 0;

                    batch = new QueryBatch(*env.Get(), inMem);

                    endReached = batch->IsEmpty();

                    return true;
                }

                bool QueryCursorImpl::IteratorHasNext(IgniteError& err)
                {
                    JniErrorInfo jniErr;

                    bool res = env.Get()->Context()->TargetInLongOutLong(javaRef, OP_ITERATOR_HAS_NEXT, 0, &jniErr) == 1;

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        return res;

                    return false;
                }
            }
        }
    }
}