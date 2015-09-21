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

#ifndef _IGNITE_IMPL
#define _IGNITE_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

#include "ignite/impl/cache/cache_impl.h"
#include "ignite/impl/ignite_environment.h"
#include "ignite/impl/utils.h"

namespace ignite 
{    
    namespace impl 
    {            
        /**
         * Ignite implementation.
         */
        class IgniteImpl
        {
            friend class Ignite;
        public:
            /**
             * Constructor used to create new instance.
             *
             * @param env Environment.
             * @param javaRef Reference to java object.
             */
            IgniteImpl(ignite::common::concurrent::SharedPointer<IgniteEnvironment> env, jobject javaRef);
            
            /**
             * Destructor.
             */
            ~IgniteImpl();

            /**
             * Get name of the Ignite.
             *
             * @param Name.
             */
            char* GetName();

            /**
             * Get cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            template<typename K, typename V> 
            cache::CacheImpl* GetCache(const char* name, IgniteError* err)
            {
                ignite::common::java::JniErrorInfo jniErr;

                jobject cacheJavaRef = env.Get()->Context()->ProcessorCache(javaRef, name, &jniErr);

                if (!cacheJavaRef)
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    return NULL;
                }

                char* name0 = utils::CopyChars(name);

                return new cache::CacheImpl(name0, env, cacheJavaRef);
            }

            /**
             * Get or create cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            template<typename K, typename V>
            cache::CacheImpl* GetOrCreateCache(const char* name, IgniteError* err)
            {
                ignite::common::java::JniErrorInfo jniErr;

                jobject cacheJavaRef = env.Get()->Context()->ProcessorGetOrCreateCache(javaRef, name, &jniErr);

                if (!cacheJavaRef)
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    return NULL;
                }

                char* name0 = utils::CopyChars(name);

                return new cache::CacheImpl(name0, env, cacheJavaRef);
            }

            /**
             * Create cache.
             *
             * @param name Cache name.
             * @param err Error.
             */
            template<typename K, typename V>
            cache::CacheImpl* CreateCache(const char* name, IgniteError* err)
            {
                ignite::common::java::JniErrorInfo jniErr;

                jobject cacheJavaRef = env.Get()->Context()->ProcessorCreateCache(javaRef, name, &jniErr);

                if (!cacheJavaRef)
                {
                    IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                    return NULL;
                }

                char* name0 = utils::CopyChars(name);

                return new cache::CacheImpl(name0, env, cacheJavaRef);
            }
        private:
            /** Environment. */
            ignite::common::concurrent::SharedPointer<IgniteEnvironment> env;
            
            /** Native Java counterpart. */
            jobject javaRef;   
            
            IGNITE_NO_COPY_ASSIGNMENT(IgniteImpl)
        };
    }
    
}

#endif