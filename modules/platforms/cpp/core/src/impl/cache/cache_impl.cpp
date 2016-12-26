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

#include <ignite/common/utils.h>

#include "ignite/impl/cache/cache_impl.h"
#include "ignite/impl/binary/binary_type_updater_impl.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::common;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::cache::query::continuous;
using namespace ignite::impl;
using namespace ignite::impl::binary;
using namespace ignite::impl::cache::query;
using namespace ignite::impl::cache::query::continuous;
using namespace ignite::impl::interop;
using namespace ignite::binary;

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

            /** Operation: CONTINUOUS query. */
            const int32_t OP_QRY_CONTINUOUS = 29;

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

            /** Operation: Clear(). */
            const int32_t OP_CLEAR_CACHE = 41;

            /** Operation: RemoveAll(). */
            const int32_t OP_REMOVE_ALL2 = 43;

            /** Operation: Size(peekModes). */
            const int32_t OP_SIZE = 48;

            /** Operation: SizeLoc(peekModes). */
            const int32_t OP_SIZE_LOC = 48;

            CacheImpl::CacheImpl(char* name, SharedPointer<IgniteEnvironment> env, jobject javaRef) :
                InteropTarget(env, javaRef),
                name(name)
            {
                // No-op.
            }

            CacheImpl::~CacheImpl()
            {
                ReleaseChars(name);

                JniContext::Release(GetTarget());
            }

            const char* CacheImpl::GetName() const
            {
                return name;
            }

            bool CacheImpl::ContainsKey(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_CONTAINS_KEY, inOp, err);
            }

            bool CacheImpl::ContainsKeys(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_CONTAINS_KEYS, inOp, err);
            }

            void CacheImpl::LocalPeek(InputOperation& inOp, OutputOperation& outOp, int32_t peekModes, IgniteError* err)
            {
                OutInOpX(OP_LOCAL_PEEK, inOp, outOp, err);
            }

            void CacheImpl::Get(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpX(OP_GET, inOp, outOp, err);
            }

            void CacheImpl::GetAll(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpX(OP_GET_ALL, inOp, outOp, err);
            }

            void CacheImpl::Put(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_PUT, inOp, err);
            }

            void CacheImpl::PutAll(ignite::impl::InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_PUT_ALL, inOp, err);
            }

            void CacheImpl::GetAndPut(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpX(OP_GET_AND_PUT, inOp, outOp, err);
            }

            void CacheImpl::GetAndReplace(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpX(OP_GET_AND_REPLACE, inOp, outOp, err);
            }

            void CacheImpl::GetAndRemove(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpX(OP_GET_AND_REMOVE, inOp, outOp, err);
            }

            bool CacheImpl::PutIfAbsent(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_PUT_IF_ABSENT, inOp, err);
            }

            void CacheImpl::GetAndPutIfAbsent(InputOperation& inOp, OutputOperation& outOp, IgniteError* err)
            {
                OutInOpX(OP_GET_AND_PUT_IF_ABSENT, inOp, outOp, err);
            }

            bool CacheImpl::Replace(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_REPLACE_2, inOp, err);
            }

            bool CacheImpl::ReplaceIfEqual(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_REPLACE_3, inOp, err);
            }

            void CacheImpl::LocalEvict(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_LOCAL_EVICT, inOp, err);
            }

            void CacheImpl::Clear(IgniteError* err)
            {
                JniErrorInfo jniErr;

                OutOp(OP_CLEAR_CACHE, err);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);
            }

            void CacheImpl::Clear(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_CLEAR, inOp, err);
            }

            void CacheImpl::ClearAll(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_CLEAR_ALL, inOp, err);
            }

            void CacheImpl::LocalClear(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_LOCAL_CLEAR, inOp, err);
            }

            void CacheImpl::LocalClearAll(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_LOCAL_CLEAR_ALL, inOp, err);
            }

            bool CacheImpl::Remove(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_REMOVE_1, inOp, err);
            }

            bool CacheImpl::RemoveIfEqual(InputOperation& inOp, IgniteError* err)
            {
                return OutOp(OP_REMOVE_2, inOp, err);
            }

            void CacheImpl::RemoveAll(InputOperation& inOp, IgniteError* err)
            {
                OutOp(OP_REMOVE_ALL, inOp, err);
            }

            void CacheImpl::RemoveAll(IgniteError* err)
            {
                JniErrorInfo jniErr;

                OutOp(OP_REMOVE_ALL2, err);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);
            }

            int32_t CacheImpl::Size(int32_t peekModes, bool local, IgniteError* err)
            {
                int32_t op = local ? OP_SIZE_LOC : OP_SIZE;

                return static_cast<int32_t>(OutInOpLong(op, peekModes, err));
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

            QueryCursorImpl* CacheImpl::QuerySqlFields(const SqlFieldsQuery& qry, IgniteError* err)
            {
                return QueryInternal(qry, OP_QRY_SQL_FIELDS, err);
            }

            ContinuousQueryHandleImpl* CacheImpl::QueryContinuous(const SharedPointer<ContinuousQueryImplBase> qry,
                const SqlQuery& initialQry, IgniteError& err)
            {
                return QueryContinuous(qry, initialQry, OP_QRY_SQL, OP_QRY_CONTINUOUS, err);
            }

            ContinuousQueryHandleImpl* CacheImpl::QueryContinuous(const SharedPointer<ContinuousQueryImplBase> qry,
                const TextQuery& initialQry, IgniteError& err)
            {
                return QueryContinuous(qry, initialQry, OP_QRY_TEXT, OP_QRY_CONTINUOUS, err);
            }

            ContinuousQueryHandleImpl* CacheImpl::QueryContinuous(const SharedPointer<ContinuousQueryImplBase> qry,
                const ScanQuery& initialQry, IgniteError& err)
            {
                return QueryContinuous(qry, initialQry, OP_QRY_SCAN, OP_QRY_CONTINUOUS, err);
            }

            struct DummyQry { void Write(BinaryRawWriter&) const { }};

            ContinuousQueryHandleImpl* CacheImpl::QueryContinuous(const SharedPointer<ContinuousQueryImplBase> qry,
                IgniteError& err)
            {
                DummyQry dummy;
                return QueryContinuous(qry, dummy, -1, OP_QRY_CONTINUOUS, err);
            }
        }
    }
}