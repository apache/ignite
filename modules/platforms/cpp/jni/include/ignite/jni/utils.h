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
#ifndef _IGNITE_JNI_UTILS
#define _IGNITE_JNI_UTILS

#include <string>

#include <ignite/jni/java.h>
#include <ignite/common/common.h>

namespace ignite
{
    namespace jni
    {
        /**
         * Helper class to manage attached threads.
         */
        class AttachHelper 
        {
        public:
            /**
             * Destructor.
             */
            ~AttachHelper();

            /**
             * Callback invoked on successful thread attach ot JVM.
             */
            static void OnThreadAttach();
        };

        /**
        * Represents global reference to Java object.
        */
        class IGNITE_IMPORT_EXPORT JavaGlobalRef
        {
        public:
            /**
            * Default constructor
            */
            JavaGlobalRef() :
                ctx(0),
                obj(0)
            {
                // No-op.
            }

            /**
            * Constructor
            *
            * @param ctx JNI context.
            * @param obj Java object.
            */
            JavaGlobalRef(java::JniContext& ctx, jobject obj) :
                ctx(&ctx),
                obj(ctx.Acquire(obj))
            {
                // No-op.
            }

            /**
            * Copy constructor
            *
            * @param other Other instance.
            */
            JavaGlobalRef(const JavaGlobalRef& other) :
                ctx(other.ctx),
                obj(ctx->Acquire(other.obj))
            {
                // No-op.
            }

            /**
            * Assignment operator.
            *
            * @param other Other instance.
            * @return *this.
            */
            JavaGlobalRef& operator=(const JavaGlobalRef& other)
            {
                if (this != &other)
                {
                    if (ctx)
                        ctx->Release(obj);

                    ctx = other.ctx;
                    obj = ctx->Acquire(other.obj);
                }

                return *this;
            }

            /**
            * Destructor.
            */
            ~JavaGlobalRef()
            {
                if (ctx)
                    ctx->Release(obj);
            }

            /**
            * Get object.
            *
            * @return Object.
            */
            jobject Get()
            {
                return obj;
            }

        private:
            /** Context. */
            java::JniContext* ctx;

            /** Object. */
            jobject obj;
        };

        /**
         * Attempts to find JVM library to load it into the process later.
         * First search is performed using the passed path argument (is not NULL).
         * Then JRE_HOME is evaluated. Last, JAVA_HOME is evaluated.
         *
         * @param Explicitly defined path (optional).
         * @param found Whether library was found.
         * @return Path to the file.
         */
        IGNITE_IMPORT_EXPORT std::string FindJvmLibrary(const std::string* path, bool* found);

        /**
         * Load JVM library into the process.
         *
         * @param path Optional path to the library.
         * @return Whether load was successful.
         */
        IGNITE_IMPORT_EXPORT bool LoadJvmLibrary(const std::string& path);

        /**
         * Create Ignite classpath based on user input and home directory.
         *
         * @param usrCp User's classpath.
         * @param home Ignite home directory.
         * @return Classpath.
         */
        IGNITE_IMPORT_EXPORT std::string CreateIgniteClasspath(const std::string* usrCp, const std::string* home);

        /**
         * Create Ignite classpath based on user input and home directory.
         *
         * @param usrCp User's classpath.
         * @param home Ignite home directory.
         * @param test Whether test classpath must be used.
         * @return Classpath.
         */
        IGNITE_IMPORT_EXPORT std::string CreateIgniteClasspath(const std::string* usrCp, const std::string* home, bool test);

        /**
         * Resolve IGNITE_HOME directory. Resolution is performed in several
         * steps:
         * 1) Check for path provided as argument.
         * 2) Check for environment variable.
         * 3) Check for current working directory.
         * Result of these 3 checks are evaluated based on existence of certain
         * predefined folders inside possible GG home. If they are found, 
         * IGNITE_HOME is considered resolved.
         *
         * @param path Optional path to evaluate.
         * @param found Whether IGNITE_HOME home was found.
         * @return Resolved GG home.
         */
        IGNITE_IMPORT_EXPORT std::string ResolveIgniteHome(const std::string* path, bool* found);
    }
}

#endif //_IGNITE_JNI_UTILS