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

#ifndef _IGNITE_CACHE_IMPL
#define _IGNITE_CACHE_IMPL

#include <ignite/cache/query/query_scan.h>
#include <ignite/cache/query/query_sql.h>
#include <ignite/cache/query/query_text.h>
#include <ignite/cache/query/query_sql_fields.h>
#include <ignite/cache/query/continuous/continuous_query_handle.h>
#include <ignite/impl/cache/query/query_impl.h>
#include <ignite/impl/cache/query/continuous/continuous_query_handle_impl.h>
#include <ignite/impl/cache/query/continuous/continuous_query_impl.h>

#include <ignite/impl/interop/interop_target.h>

namespace ignite
{    
    namespace impl 
    {
        namespace cache
        {
            /**
             * Cache implementation.
             */
            class IGNITE_IMPORT_EXPORT CacheImpl : private interop::InteropTarget
            {
            public:
                /**
                 * Constructor used to create new instance.
                 *
                 * @param name Name.
                 * @param env Environment.
                 * @param javaRef Reference to java object.
                 */
                CacheImpl(char* name, ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);
                
                /**
                 * Destructor.
                 */
                ~CacheImpl();
                
                /**
                 * Get name.
                 *
                 * @return Cache name.
                 */
                const char* GetName() const;

                /**
                 * Perform ContainsKey.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result.
                 */
                bool ContainsKey(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform ContainsKeys.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result.
                 */
                bool ContainsKeys(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform LocalPeek.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param peekModes Peek modes.
                 * @param err Error.
                 */
                void LocalPeek(InputOperation& inOp, OutputOperation& outOp, 
                    int32_t peekModes, IgniteError* err);

                /**
                 * Perform Get.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void Get(InputOperation& inOp, OutputOperation& outOp, IgniteError* err);
                
                /**
                 * Perform GetAll.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void GetAll(InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                 * Perform Put.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void Put(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform PutAll.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void PutAll(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform GetAndPut.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void GetAndPut(InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                 * Perform GetAndReplace.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void GetAndReplace(InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                 * Perform GetAndRemove.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void GetAndRemove(InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                 * Perform PutIfAbsent.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result
                 */
                bool PutIfAbsent(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform GetAndPutIfAbsent.
                 *
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void GetAndPutIfAbsent(InputOperation& inOp, OutputOperation& outOp, IgniteError* err);

                /**
                 * Perform Replace(K, V).
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result
                 */
                bool Replace(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform Replace(K, V, V).
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result
                 */
                bool ReplaceIfEqual(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform LocalEvict.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void LocalEvict(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform Clear.
                 *
                 * @param err Error.
                 */
                void Clear(IgniteError* err);

                /**
                 * Perform Clear.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void Clear(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform ClearAll.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void ClearAll(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform LocalClear.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void LocalClear(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform LocalClearAll.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void LocalClearAll(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform Remove(K).
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result
                 */
                bool Remove(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform Remove(K, V).
                 *
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result
                 */
                bool RemoveIfEqual(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform RemoveAll.
                 *
                 * @param inOp Input.
                 * @param err Error.
                 */
                void RemoveAll(InputOperation& inOp, IgniteError* err);

                /**
                 * Perform RemoveAll.
                 *
                 * @param err Error.
                 */
                void RemoveAll(IgniteError* err);

                /**
                * Perform Size.
                *
                * @param peekModes Peek modes.
                * @param local Local flag.
                * @param err Error.
                */
                int32_t Size(int32_t peekModes, bool local, IgniteError* err);

                /**
                 * Invoke query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QuerySql(const ignite::cache::query::SqlQuery& qry, IgniteError* err);

                /**
                 * Invoke text query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QueryText(const ignite::cache::query::TextQuery& qry, IgniteError* err);

                /**
                 * Invoke scan query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QueryScan(const ignite::cache::query::ScanQuery& qry, IgniteError* err);

                /**
                 * Invoke sql fields query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QuerySqlFields(const ignite::cache::query::SqlFieldsQuery& qry, IgniteError* err);

                /**
                 * Start continuous query execution.
                 *
                 * @param qry Continuous query.
                 * @param err Error.
                 * @return Continuous query handle.
                 */
                query::continuous::ContinuousQueryHandleImpl* QueryContinuous(
                    const common::concurrent::SharedPointer<query::continuous::ContinuousQueryImplBase> qry,
                    IgniteError& err);

                /**
                 * Start continuous query execution with initial query.
                 *
                 * @param qry Continuous query.
                 * @param initialQry Initial query.
                 * @param err Error.
                 * @return Continuous query handle.
                 */
                query::continuous::ContinuousQueryHandleImpl* QueryContinuous(
                    const common::concurrent::SharedPointer<query::continuous::ContinuousQueryImplBase> qry,
                    const ignite::cache::query::SqlQuery& initialQry, IgniteError& err);

                /**
                 * Start continuous query execution with initial query.
                 *
                 * @param qry Continuous query.
                 * @param initialQry Initial query.
                 * @param err Error.
                 * @return Continuous query handle.
                 */
                query::continuous::ContinuousQueryHandleImpl* QueryContinuous(
                    const common::concurrent::SharedPointer<query::continuous::ContinuousQueryImplBase> qry,
                    const ignite::cache::query::TextQuery& initialQry, IgniteError& err);

                /**
                 * Start continuous query execution with initial query.
                 *
                 * @param qry Continuous query.
                 * @param initialQry Initial query.
                 * @param err Error.
                 * @return Continuous query handle.
                 */
                query::continuous::ContinuousQueryHandleImpl* QueryContinuous(
                    const common::concurrent::SharedPointer<query::continuous::ContinuousQueryImplBase> qry,
                    const ignite::cache::query::ScanQuery& initialQry, IgniteError& err);

            private:
                IGNITE_NO_COPY_ASSIGNMENT(CacheImpl)

                /** Name. */
                char* name; 

                /**
                 * Internal query execution routine.
                 *
                 * @param qry Query.
                 * @param typ Query type.
                 * @param err Error.
                 */
                template<typename T>
                query::QueryCursorImpl* QueryInternal(const T& qry, int32_t typ, IgniteError* err)
                {
                    ignite::jni::java::JniErrorInfo jniErr;

                    ignite::common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropMemory* mem0 = mem.Get();
                    interop::InteropOutputStream out(mem0);
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());
                    ignite::binary::BinaryRawWriter rawWriter(&writer);

                    qry.Write(rawWriter);

                    out.Synchronize();

                    jobject qryJavaRef = GetEnvironment().Context()->CacheOutOpQueryCursor(GetTarget(),
                        typ, mem.Get()->PointerLong(), &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == ignite::java::IGNITE_JNI_ERR_SUCCESS)
                        return new query::QueryCursorImpl(GetEnvironmentPointer(), qryJavaRef);
                    else
                        return 0;
                }

                /**
                 * Start continuous query execution with the initial query.
                 *
                 * @param qry Continuous query.
                 * @param initialQry Initial query to be executed.
                 * @param err Error.
                 * @return Continuous query handle.
                 */
                template<typename T>
                query::continuous::ContinuousQueryHandleImpl* QueryContinuous(
                    const common::concurrent::SharedPointer<query::continuous::ContinuousQueryImplBase> qry,
                    const T& initialQry, int32_t typ, int32_t cmd, IgniteError& err)
                {
                    jni::java::JniErrorInfo jniErr;

                    common::concurrent::SharedPointer<interop::InteropMemory> mem = GetEnvironment().AllocateMemory();
                    interop::InteropMemory* mem0 = mem.Get();
                    interop::InteropOutputStream out(mem0);
                    binary::BinaryWriterImpl writer(&out, GetEnvironment().GetTypeManager());
                    ignite::binary::BinaryRawWriter rawWriter(&writer);

                    const query::continuous::ContinuousQueryImplBase& qry0 = *qry.Get();

                    int64_t handle = GetEnvironment().GetHandleRegistry().Allocate(qry);

                    rawWriter.WriteInt64(handle);
                    rawWriter.WriteBool(qry0.GetLocal());

                    // Filters are not supported for now.
                    rawWriter.WriteBool(false);
                    rawWriter.WriteNull();

                    rawWriter.WriteInt32(qry0.GetBufferSize());
                    rawWriter.WriteInt64(qry0.GetTimeInterval());

                    // Autounsubscribe is a filter feature.
                    rawWriter.WriteBool(false);

                    // Writing initial query. When there is not initial query writing -1.
                    rawWriter.WriteInt32(typ);
                    if (typ != -1)
                        initialQry.Write(rawWriter);

                    out.Synchronize();

                    jobject qryJavaRef = GetEnvironment().Context()->CacheOutOpContinuousQuery(GetTarget(),
                        cmd, mem.Get()->PointerLong(), &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                    if (jniErr.code == java::IGNITE_JNI_ERR_SUCCESS)
                        return new query::continuous::ContinuousQueryHandleImpl(GetEnvironmentPointer(), handle, qryJavaRef);

                    return 0;
                }
            };
        }
    }    
}

#endif
