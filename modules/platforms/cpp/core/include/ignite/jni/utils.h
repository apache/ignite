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

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>

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
                obj(NULL)
            {
                // No-op.
            }

            /**
             * Constructor
             *
             * @param ctx JNI context.
             * @param obj Java object.
             */
            JavaGlobalRef(common::concurrent::SharedPointer<java::JniContext>& ctx, jobject obj)
                : obj(NULL)
            {
                Init(ctx, obj);
            }

            /**
             * Copy constructor
             *
             * @param other Other instance.
             */
            JavaGlobalRef(const JavaGlobalRef& other)
                : obj(NULL)
            {
                Init(other.ctx, other.obj);
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
                    java::JniContext::Release(obj);

                    Init(other.ctx, other.obj);
                }

                return *this;
            }

            /**
             * Destructor.
             */
            ~JavaGlobalRef()
            {
                java::JniContext::Release(obj);
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
            /** Initializer */
            void Init(const common::concurrent::SharedPointer<java::JniContext>& ctx0, jobject obj0) {
                ctx = ctx0;

                if (ctx.IsValid())
                    this->obj = ctx.Get()->Acquire(obj0);
            }

            /** Context. */
            common::concurrent::SharedPointer<java::JniContext> ctx;

            /** Object. */
            jobject obj;
        };

        /**
         * Attempts to find JVM library to load it into the process later.
         * First search is performed using the passed path argument (is not NULL).
         * Then JRE_HOME is evaluated. Last, JAVA_HOME is evaluated.
         *
         * @param path Explicitly defined path (optional).
         * @return Path to the file. Empty string if the library was not found.
         */
        IGNITE_IMPORT_EXPORT std::string FindJvmLibrary(const std::string& path);

        /**
         * Load JVM library into the process.
         *
         * @param path Optional path to the library.
         * @return Whether load was successful.
         */
        IGNITE_IMPORT_EXPORT bool LoadJvmLibrary(const std::string& path);

        /**
         * Helper function to create classpath based on Ignite home directory.
         *
         * @param home Home directory; expected to be valid.
         * @param forceTest Force test classpath.
         * @return Classpath.
         */
        IGNITE_IMPORT_EXPORT std::string CreateIgniteHomeClasspath(const std::string& home, bool forceTest);

        /**
         * Create Ignite classpath based on user input and home directory.
         *
         * @param usrCp User's classpath.
         * @param home Ignite home directory.
         * @return Classpath.
         */
        IGNITE_IMPORT_EXPORT std::string CreateIgniteClasspath(const std::string& usrCp, const std::string& home);

        /**
         * Resolve IGNITE_HOME directory. Resolution is performed in several
         * steps:
         * 1) Check for path provided as argument.
         * 2) Check for environment variable.
         * 3) Check for current working directory.
         * Result of these checks are evaluated based on existence of certain
         * predefined folders inside possible Ignite home. If they are found,
         * IGNITE_HOME is considered resolved.
         *
         * @param path Optional path to evaluate.
         * @return Resolved Ignite home.
         */
        IGNITE_IMPORT_EXPORT std::string ResolveIgniteHome(const std::string& path = "");
    }
}

#endif //_IGNITE_JNI_UTILS
