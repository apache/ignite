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

#ifndef _IGNITE_IMPL_IGNITE_IMPL
#define _IGNITE_IMPL_IGNITE_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>
#include <ignite/common/utils.h>

#include "ignite/impl/cache/cache_impl.h"
#include "ignite/impl/transactions/transactions_impl.h"
#include "ignite/impl/ignite_environment.h"

namespace ignite 
{
    namespace impl 
    {
        /**
         * Ignite implementation.
         */
        class IGNITE_FRIEND_EXPORT IgniteImpl
        {
            typedef ignite::common::concurrent::SharedPointer<IgniteEnvironment> SP_IgniteEnvironment;
            typedef ignite::common::concurrent::SharedPointer<impl::transactions::TransactionsImpl> SP_TransactionsImpl;
        public:
            /**
             * Constructor used to create new instance.
             *
             * @param env Environment.
             * @param javaRef Reference to java object.
             */
            IgniteImpl(SP_IgniteEnvironment env, jobject javaRef);
            
            /**
             * Destructor.
             */
            ~IgniteImpl();

            /**
             * Get name of the Ignite.
             *
             * @param Name.
             */
            const char* GetName() const;

            /**
             * Get JNI context associated with this instance.
             *
             * @return JNI context for this instance.
             */
            jni::java::JniContext* GetContext();

            /**
             * Get cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            template<typename K, typename V> 
            cache::CacheImpl* GetCache(const char* name, IgniteError& err)
            {
                ignite::jni::java::JniErrorInfo jniErr;

                jobject cacheJavaRef = env.Get()->Context()->ProcessorCache(javaRef, name, &jniErr);

                if (!cacheJavaRef)
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                    return NULL;
                }

                char* name0 = common::CopyChars(name);

                return new cache::CacheImpl(name0, env, cacheJavaRef);
            }

            /**
             * Get or create cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            template<typename K, typename V>
            cache::CacheImpl* GetOrCreateCache(const char* name, IgniteError& err)
            {
                ignite::jni::java::JniErrorInfo jniErr;

                jobject cacheJavaRef = env.Get()->Context()->ProcessorGetOrCreateCache(javaRef, name, &jniErr);

                if (!cacheJavaRef)
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                    return NULL;
                }

                char* name0 = common::CopyChars(name);

                return new cache::CacheImpl(name0, env, cacheJavaRef);
            }

            /**
             * Create cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            template<typename K, typename V>
            cache::CacheImpl* CreateCache(const char* name, IgniteError& err)
            {
                ignite::jni::java::JniErrorInfo jniErr;

                jobject cacheJavaRef = env.Get()->Context()->ProcessorCreateCache(javaRef, name, &jniErr);

                if (!cacheJavaRef)
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, &err);

                    return NULL;
                }

                char* name0 = common::CopyChars(name);

                return new cache::CacheImpl(name0, env, cacheJavaRef);
            }

            /**
             * Get instance of the implementation from the proxy class.
             * Internal method. Should not be used by user.
             *
             * @param proxy Proxy instance containing IgniteImpl.
             * @return IgniteImpl instance associated with the proxy or null-pointer.
             */
            template<typename T>
            static IgniteImpl* GetFromProxy(T& proxy)
            {
                return proxy.impl.Get();
            }

            /**
             * Get transactions.
             *
             * @return TransactionsImpl instance.
             */
            SP_TransactionsImpl GetTransactions()
            {
                return txImpl;
            }

        private:
            /**
             * Get transactions internal call.
             *
             * @return TransactionsImpl instance.
             */
            SP_TransactionsImpl InternalGetTransactions(IgniteError &err);

            /** Environment. */
            SP_IgniteEnvironment env;

            /** Native Java counterpart. */
            jobject javaRef;

            /** Transactions implementaion. */
            SP_TransactionsImpl txImpl;

            IGNITE_NO_COPY_ASSIGNMENT(IgniteImpl)
        };
    }
}

#endif //_IGNITE_IMPL_IGNITE_IMPL