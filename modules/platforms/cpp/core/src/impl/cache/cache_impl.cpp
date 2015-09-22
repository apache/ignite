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

#include "ignite/cache/cache_peek_mode.h"
#include "ignite/impl/cache/cache_impl.h"
#include "ignite/impl/interop/interop.h"
#include "ignite/impl/portable/portable_reader_impl.h"
#include "ignite/impl/utils.h"
#include "ignite/impl/portable/portable_metadata_updater_impl.h"
#include "ignite/portable/portable.h"

using namespace ignite::common::concurrent;
using namespace ignite::common::java;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::impl;
using namespace ignite::impl::cache::query;
using namespace ignite::impl::interop;
using namespace ignite::impl::portable;
using namespace ignite::impl::utils;
using namespace ignite::portable;

namespace ignite
{
    namespace impl
    {
        namespace cache
        {
            /** Operation: Clear. */
            const int32_t OP_CLEAR = 1;

            /** Operation: ClearAll. */
            const int32_t OP_CLEAR_ALL = 2;

            /** Operation: ContainsKey. */
            const int32_t OP_CONTAINS_KEY = 3;

            /** Operation: ContainsKeys. */
            const int32_t OP_CONTAINS_KEYS = 4;

            /** Operation: Get. */
            const int32_t OP_GET = 5;

            /** Operation: GetAll. */
            const int32_t OP_GET_ALL = 6;

            /** Operation: GetAndPut. */
            const int32_t OP_GET_AND_PUT = 7;

            /** Operation: GetAndPutIfAbsent. */
            const int32_t OP_GET_AND_PUT_IF_ABSENT = 8;

            /** Operation: GetAndRemove. */
            const int32_t OP_GET_AND_REMOVE = 9;

            /** Operation: GetAndReplace. */
            const int32_t OP_GET_AND_REPLACE = 10;

            /** Operation: LocalEvict. */
            const int32_t OP_LOCAL_EVICT = 16;

            /** Operation: LocalClear. */
            const int32_t OP_LOCAL_CLEAR = 20;

            /** Operation: LocalClearAll. */
            const int32_t OP_LOCAL_CLEAR_ALL = 21;

            /** Operation: LocalPeek. */
            const int32_t OP_LOCAL_PEEK = 25;

            /** Operation: Put. */
            const int32_t OP_PUT = 26;

            /** Operation: PutAll. */
            const int32_t OP_PUT_ALL = 27;

            /** Operation: PutIfAbsent. */
            const int32_t OP_PUT_IF_ABSENT = 28;

            /** Operation: SCAN query. */
            const int32_t OP_QRY_SCAN = 30;

            /** Operation: SQL query. */
            const int32_t OP_QRY_SQL = 31;

            /** Operation: SQL fields query. */
            const int32_t OP_QRY_SQL_FIELDS = 32;

            /** Operation: TEXT query. */
            const int32_t OP_QRY_TEXT = 33;

            /** Operation: RemoveAll. */
            const int32_t OP_REMOVE_ALL = 34;

            /** Operation: Remove(K, V). */
            const int32_t OP_REMOVE_2 = 35;

            /** Operation: Remove(K). */
            const int32_t OP_REMOVE_1 = 36;

            /** Operation: Replace(K, V). */
            const int32_t OP_REPLACE_2 = 37;

            /** Operation: Replace(K, V, V). */
            const int32_t OP_REPLACE_3 = 38;

            CacheImpl::CacheImpl(char* name, SharedPointer<IgniteEnvironment> env, jobject javaRef) :
                name(name), env(env), javaRef(javaRef)
            {
                // No-op.
            }

            CacheImpl::~CacheImpl()
            {
                ReleaseChars(name);

                JniContext::Release(javaRef);
            }

            char* CacheImpl::GetName()
            {
                return name;
            }

            bool CacheImpl::IsEmpty(IgniteError* err)
            {
                return Size(IGNITE_PEEK_MODE_ALL, err) == 0;
            }

            bool CacheImpl::ContainsKey(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_CONTAINS_KEY, inOp, err);
            }

            bool CacheImpl::ContainsKeys(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_CONTAINS_KEYS, inOp, err);
            }

            void CacheImpl::LocalPeek(InputOperation& inOp, OutputOperation& outOp, int32_t peekModes, IgniteError* err)
            {
                OutInOpInternal(OP_LOCAL_PEEK, inOp, outOp, err);
            }

            void CacheImpl::Get(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpInternal(OP_GET, inOp, outOp, err);
            }

            void CacheImpl::GetAll(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpInternal(OP_GET_ALL, inOp, outOp, err);
            }

            void CacheImpl::Put(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_PUT, inOp, err);
            }

            void CacheImpl::PutAll(ignite::impl::InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_PUT_ALL, inOp, err);
            }

            void CacheImpl::GetAndPut(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpInternal(OP_GET_AND_PUT, inOp, outOp, err);
            }

            void CacheImpl::GetAndReplace(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpInternal(OP_GET_AND_REPLACE, inOp, outOp, err);
            }

            void CacheImpl::GetAndRemove(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpInternal(OP_GET_AND_REMOVE, inOp, outOp, err);
            }

            bool CacheImpl::PutIfAbsent(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_PUT_IF_ABSENT, inOp, err);
            }

            void CacheImpl::GetAndPutIfAbsent(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpInternal(OP_GET_AND_PUT_IF_ABSENT, inOp, outOp, err);
            }

            bool CacheImpl::Replace(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_REPLACE_2, inOp, err);
            }

            bool CacheImpl::ReplaceIfEqual(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_REPLACE_3, inOp, err);
            }

            void CacheImpl::LocalEvict(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_LOCAL_EVICT, inOp, err);
            }

            void CacheImpl::Clear(IgniteError* err)
            {
                JniErrorInfo jniErr;

                env.Get()->Context()->CacheClear(javaRef, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);
            }

            void CacheImpl::Clear(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_CLEAR, inOp, err);
            }

            void CacheImpl::ClearAll(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_CLEAR_ALL, inOp, err);
            }

            void CacheImpl::LocalClear(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_LOCAL_CLEAR, inOp, err);
            }

            void CacheImpl::LocalClearAll(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_LOCAL_CLEAR_ALL, inOp, err);
            }

            bool CacheImpl::Remove(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_REMOVE_1, inOp, err);
            }

            bool CacheImpl::RemoveIfEqual(InputOperation& inOp, IgniteError* err)
            {
                return OutOpInternal(OP_REMOVE_2, inOp, err);
            }

            void CacheImpl::RemoveAll(InputOperation& inOp, IgniteError* err)
            {
                OutOpInternal(OP_REMOVE_ALL, inOp, err);
            }

            void CacheImpl::RemoveAll(IgniteError* err)
            {
                JniErrorInfo jniErr;

                env.Get()->Context()->CacheRemoveAll(javaRef, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);
            }

            int32_t CacheImpl::Size(const int32_t peekModes, IgniteError* err)
            {
                return SizeInternal(peekModes, false, err);
            }

            int32_t CacheImpl::LocalSize(const int32_t peekModes, IgniteError* err)
            {
                return SizeInternal(peekModes, true, err);
            }

            QueryCursorImpl* CacheImpl::QuerySql(const SqlQuery& qry, IgniteError* err)
            {
                return QueryInternal(qry, OP_QRY_SQL, err);
            }

            QueryCursorImpl* CacheImpl::QueryText(const TextQuery& qry, IgniteError* err)
            {
                return QueryInternal(qry, OP_QRY_TEXT, err);
            }

            QueryCursorImpl* CacheImpl::QueryScan(const ScanQuery& qry, IgniteError* err)
            {
                return QueryInternal(qry, OP_QRY_SCAN, err);
            }

            int64_t CacheImpl::WriteTo(InteropMemory* mem, InputOperation& inOp, IgniteError* err)
            {
                PortableMetadataManager* metaMgr = env.Get()->GetMetadataManager();

                int32_t metaVer = metaMgr->GetVersion();

                InteropOutputStream out(mem);
                PortableWriterImpl writer(&out, metaMgr);
                
                inOp.ProcessInput(writer);

                out.Synchronize();

                if (metaMgr->IsUpdatedSince(metaVer))
                {
                    PortableMetadataUpdaterImpl metaUpdater(env, javaRef);

                    if (!metaMgr->ProcessPendingUpdates(&metaUpdater, err))
                        return 0;
                }

                return mem->PointerLong();
            }

            void CacheImpl::ReadFrom(InteropMemory* mem, OutputOperation& outOp)
            {
                InteropInputStream in(mem);

                PortableReaderImpl reader(&in);

                outOp.ProcessOutput(reader);
            }

            int CacheImpl::SizeInternal(const int32_t peekModes, const bool loc, IgniteError* err)
            {
                JniErrorInfo jniErr;

                int res = env.Get()->Context()->CacheSize(javaRef, peekModes, loc, &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                    return res;
                else
                    return -1;
            }

            bool CacheImpl::OutOpInternal(const int32_t opType, InputOperation& inOp, IgniteError* err)
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

            void CacheImpl::OutInOpInternal(const int32_t opType, InputOperation& inOp, OutputOperation& outOp, 
                IgniteError* err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> outMem = env.Get()->AllocateMemory();
                SharedPointer<InteropMemory> inMem = env.Get()->AllocateMemory();

                int64_t outPtr = WriteTo(outMem.Get(), inOp, err);

                if (outPtr)
                {
                    env.Get()->Context()->TargetInStreamOutStream(javaRef, opType, WriteTo(outMem.Get(), inOp, err), 
                        inMem.Get()->PointerLong(), &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == IGNITE_JNI_ERR_SUCCESS)
                        ReadFrom(inMem.Get(), outOp);
                }
            }
        }
    }
}