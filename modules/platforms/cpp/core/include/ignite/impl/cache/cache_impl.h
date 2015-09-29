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

#include "ignite/cache/query/query_scan.h"
#include "ignite/cache/query/query_sql.h"
#include "ignite/cache/query/query_text.h"
#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/cache/query/query_impl.h"
#include "ignite/impl/operations.h"

namespace ignite
{    
    namespace impl 
    {
        namespace cache
        {
            /**
             * Cache implementation.
             */
            class IGNITE_IMPORT_EXPORT CacheImpl
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
                char* GetName();

                /**
                 * Perform IsEmpty.
                 *
                 * @param err Error.
                 * @return Result.
                 */
                bool IsEmpty(IgniteError* err);

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
                 * @param err Error.
                 * @return Result.
                 */
                int32_t Size(const int32_t peekModes, IgniteError* err);

                /**
                 * Perform LocalSize.
                 * 
                 * @param peekModes Peek modes.
                 * @param err Error.
                 * @return Result.
                 */
                int32_t LocalSize(const int32_t peekModes, IgniteError* err);

                /**
                 * Invoke query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QuerySql(const ignite::cache::query::SqlQuery& qry, IgniteError* err);

                /*
                 * Invoke text query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QueryText(const ignite::cache::query::TextQuery& qry, IgniteError* err);

                /*
                 * Invoke scan query.
                 *
                 * @param qry Query.
                 * @param err Error.
                 * @return Query cursor.
                 */
                query::QueryCursorImpl* QueryScan(const ignite::cache::query::ScanQuery& qry, IgniteError* err);
                
            private:
                /** Name. */
                char* name; 
                
                /** Environment. */
                ignite::common::concurrent::SharedPointer<IgniteEnvironment> env;
                
                /** Handle to Java object. */
                jobject javaRef;                     

                IGNITE_NO_COPY_ASSIGNMENT(CacheImpl)

                /**
                 * Write data to memory.
                 *
                 * @param mem Memory.
                 * @param inOp Input opeartion.
                 * @param err Error.
                 * @return Memory pointer.
                 */
                int64_t WriteTo(interop::InteropMemory* mem, InputOperation& inOp, IgniteError* err);

                /**
                 * Read data from memory.
                 *
                 * @param mem Memory.
                 * @param outOp Output operation.
                 */
                void ReadFrom(interop::InteropMemory* mem, OutputOperation& outOp);

                /**
                 * Internal cache size routine.
                 *
                 * @param peekModes Peek modes.
                 * @param loc Local flag.
                 * @param err Error.
                 * @return Size.
                 */
                int SizeInternal(const int32_t peekModes, const bool loc, IgniteError* err);

                /**
                 * Internal out operation.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param err Error.
                 * @return Result.
                 */
                bool OutOpInternal(const int32_t opType, InputOperation& inOp, IgniteError* err);

                /**
                 * Internal out-in operation.
                 *
                 * @param opType Operation type.
                 * @param inOp Input.
                 * @param outOp Output.
                 * @param err Error.
                 */
                void OutInOpInternal(const int32_t opType, InputOperation& inOp, OutputOperation& outOp, 
                    IgniteError* err);

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
                    ignite::common::java::JniErrorInfo jniErr;

                    ignite::common::concurrent::SharedPointer<interop::InteropMemory> mem = env.Get()->AllocateMemory();
                    interop::InteropMemory* mem0 = mem.Get();
                    interop::InteropOutputStream out(mem0);
                    portable::PortableWriterImpl writer(&out, env.Get()->GetMetadataManager());
                    ignite::portable::PortableRawWriter rawWriter(&writer);

                    qry.Write(rawWriter);

                    out.Synchronize();

                    jobject qryJavaRef = env.Get()->Context()->CacheOutOpQueryCursor(javaRef, typ, mem.Get()->PointerLong(), 
                        &jniErr);

                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    if (jniErr.code == ignite::common::java::IGNITE_JNI_ERR_SUCCESS)
                        return new query::QueryCursorImpl(env, qryJavaRef);
                    else
                        return NULL;
                }
            };
        }
    }    
}

#endif