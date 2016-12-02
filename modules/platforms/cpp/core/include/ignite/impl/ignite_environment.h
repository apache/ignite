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

#ifndef _IGNITE_IMPL_IGNITE_ENVIRONMENT
#define _IGNITE_IMPL_IGNITE_ENVIRONMENT

#include <ignite/common/concurrent.h>
#include <ignite/jni/java.h>
#include <ignite/jni/utils.h>

#include "ignite/impl/interop/interop_memory.h"
#include "ignite/impl/binary/binary_type_manager.h"
#include "ignite/impl/module_manager.h"
#include "ignite/impl/invoke_manager_impl.h"

namespace ignite 
{
    namespace impl 
    {
        /**
         * Defines environment in which Ignite operates.
         */
        class IGNITE_IMPORT_EXPORT IgniteEnvironment
        {
        public:
            /**
             * Default memory block allocation size.
             */
            enum { DEFAULT_ALLOCATION_SIZE = 1024 };

            /**
             * Default constructor.
             */
            IgniteEnvironment();

            /**
             * Destructor.
             */
            ~IgniteEnvironment();

            /**
             * Populate callback handlers.
             *
             * @param target (current env wrapped into a shared pointer).
             * @return JNI handlers.
             */
            ignite::jni::java::JniHandlers GetJniHandlers(ignite::common::concurrent::SharedPointer<IgniteEnvironment>* target);

            /**
             * Set context.
             *
             * @param ctx Context.
             */
            void SetContext(ignite::common::concurrent::SharedPointer<ignite::jni::java::JniContext> ctx);

            /**
             * Perform initialization on successful start.
             */
            void Initialize();

            /**
             * Start callback.
             *
             * @param memPtr Memory pointer.
             * @param proc Processor instance.
             */
            void OnStartCallback(long long memPtr, void* proc);

            /**
             * Cache Invoke callback.
             *
             * @param inMemPtr Input memory pointer.
             * @param outMemPtr Output memory pointer.
             */
            void CacheInvokeCallback(long long inMemPtr, long long outMemPtr);

            /**
             * Get name of Ignite instance.
             *
             * @return Name.
             */
            const char* InstanceName() const;

            /**
             * Get processor associated with the instance.
             *
             * @return Processor.
             */
            void* GetProcessor();

            /**
             * Get JNI context.
             *
             * @return Context.
             */
            ignite::jni::java::JniContext* Context();

            /**
             * Get memory for interop operations.
             *
             * @return Memory.
             */
            ignite::common::concurrent::SharedPointer<interop::InteropMemory> AllocateMemory();

            /**
             * Get memory chunk for interop operations with desired capacity.
             *
             * @param cap Capacity.
             * @return Memory.
             */
            ignite::common::concurrent::SharedPointer<interop::InteropMemory> AllocateMemory(int32_t cap);

            /**
             * Get memory chunk located at the given pointer.
             *
             * @param memPtr Memory pointer.
             * @retrun Memory.
             */
            ignite::common::concurrent::SharedPointer<interop::InteropMemory> GetMemory(int64_t memPtr);

            /**
             * Get type manager.
             *
             * @return Type manager.
             */
            binary::BinaryTypeManager* GetTypeManager();

            /**
             * Acquire ownership for the object.
             *
             * @param obj Java object to acquire.
             */
            void* Acquire(void* obj);

            /**
             * Notify processor that Ignite instance has started.
             */
            void ProcessorReleaseStart();

        private:
            /** Context to access Java. */
            ignite::common::concurrent::SharedPointer<ignite::jni::java::JniContext> ctx;

            /** Startup latch. */
            ignite::common::concurrent::SingleLatch latch;

            /** Ignite name. */
            char* name;

            /** Processor instance. */
            ignite::jni::JavaGlobalRef proc;

            /** Type manager. */
            binary::BinaryTypeManager* metaMgr;

            /** Invoke manager */
            ignite::common::concurrent::SharedPointer<InvokeManagerImpl> invokeMgr;

            /** Module manager. */
            ModuleManager* moduleMgr;

            IGNITE_NO_COPY_ASSIGNMENT(IgniteEnvironment);
        };
    }
}

#endif //_IGNITE_IMPL_IGNITE_ENVIRONMENT