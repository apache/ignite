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

#include "ignite/impl/interop/interop_target.h"
#include "ignite/impl/binary/binary_type_updater_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::binary;
using namespace ignite::binary;

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
            /**
             * Operation result.
             */
            enum OperationResult
            {
                /** Null. */
                ResultNull = 0,

                /** Object. */
                ResultObject = 1,

                /** Error. */
                ResultError = -1
            };

            InteropTarget::InteropTarget(SharedPointer<IgniteEnvironment> env, jobject javaRef) :
                env(env), javaRef(javaRef)
            {
                // No-op.
            }

            InteropTarget::~InteropTarget()
            {
                JniContext::Release(javaRef);
            }

            int64_t InteropTarget::WriteTo(InteropMemory* mem, InputOperation& inOp, IgniteError* err)
            {
                BinaryTypeManager* metaMgr = env.Get()->GetTypeManager();

                int32_t metaVer = metaMgr->GetVersion();

                InteropOutputStream out(mem);
                BinaryWriterImpl writer(&out, metaMgr);
                
                inOp.ProcessInput(writer);

                out.Synchronize();

                if (metaMgr->IsUpdatedSince(metaVer))
                {
                    if (!metaMgr->ProcessPendingUpdates(env.Get()->GetTypeUpdater(), err))
                        return 0;
                }

                return mem->PointerLong();
            }

            void InteropTarget::ReadFrom(InteropMemory* mem, OutputOperation& outOp)
            {
                InteropInputStream in(mem);

                BinaryReaderImpl reader(&in);

                outOp.ProcessOutput(reader);
            }

            bool InteropTarget::OutOp(int32_t opType, InputOperation& inOp, IgniteError* err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> mem = env.Get()->AllocateMemory();

                int64_t outPtr = WriteTo(mem.Get(), inOp, err);

                if (outPtr)
                {
                    long long res = env.Get()->Context()->TargetInStreamOutLong(javaRef, opType, outPtr, &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        return res == 1;
                }

                return false;
            }

            bool InteropTarget::OutOp(int32_t opType, IgniteError* err)
            {
                JniErrorInfo jniErr;

                long long res = env.Get()->Context()->TargetInLongOutLong(javaRef, opType, 0, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                    return res == 1;

                return false;
            }

            bool InteropTarget::InOp(int32_t opType, OutputOperation& outOp, IgniteError* err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> mem = env.Get()->AllocateMemory();

                env.Get()->Context()->TargetOutStream(javaRef, opType, mem.Get()->PointerLong(), &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                {
                    ReadFrom(mem.Get(), outOp);

                    return true;
                }

                return false;
            }

            void InteropTarget::OutInOp(int32_t opType, InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> outMem = env.Get()->AllocateMemory();
                SharedPointer<InteropMemory> inMem = env.Get()->AllocateMemory();

                int64_t outPtr = WriteTo(outMem.Get(), inOp, err);

                if (outPtr)
                {
                    env.Get()->Context()->TargetInStreamOutStream(javaRef, opType, outPtr,
                        inMem.Get()->PointerLong(), &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        ReadFrom(inMem.Get(), outOp);
                }
            }

            void InteropTarget::OutInOpX(int32_t opType, InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> outInMem = env.Get()->AllocateMemory();

                int64_t outInPtr = WriteTo(outInMem.Get(), inOp, err);

                if (outInPtr)
                {
                    int64_t res = env.Get()->Context()->TargetInStreamOutLong(javaRef, opType, outInPtr, &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS && res == ResultObject)
                        ReadFrom(outInMem.Get(), outOp);
                    else if (res == ResultNull)
                        outOp.SetNull();

                    //Read and process error if res == ResultError here.
                }
            }

            int64_t InteropTarget::OutInOpLong(int32_t opType, int64_t val, IgniteError* err)
            {
                JniErrorInfo jniErr;

                long long res = env.Get()->Context()->TargetInLongOutLong(javaRef, opType, val, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                return res;
            }
        }
    }
}