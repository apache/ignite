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
#include "ignite/impl/handle_registry.h"

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
             * Default fast path handle registry containers capasity.
             */
            enum { DEFAULT_FAST_PATH_CONTAINERS_CAP = 1024 };

            /**
            * Default slow path handle registry containers capasity.
            */
            enum { DEFAULT_SLOW_PATH_CONTAINERS_CAP = 16 };

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
            jni::java::JniHandlers GetJniHandlers(common::concurrent::SharedPointer<IgniteEnvironment>* target);

            /**
             * Set context.
             *
             * @param ctx Context.
             */
            void SetContext(common::concurrent::SharedPointer<jni::java::JniContext> ctx);

            /**
             * Perform initialization on successful start.
             */
            void Initialize();

            /**
             * Start callback.
             *
             * @param memPtr Memory pointer.
             */
            void OnStartCallback(long long memPtr, jobject proc);

            /**
             * Continuous query listener apply callback.
             *
             * @param mem Memory with data.
             */
            void OnContinuousQueryListenerApply(common::concurrent::SharedPointer<interop::InteropMemory>& mem);

            /**
             * Get name of Ignite instance.
             *
             * @return Name.
             */
            const char* InstanceName() const;

            /**
             * Get JNI context.
             *
             * @return Context.
             */
            jni::java::JniContext* Context();

            /**
             * Get memory for interop operations.
             *
             * @return Memory.
             */
            common::concurrent::SharedPointer<interop::InteropMemory> AllocateMemory();

            /**
             * Get memory chunk for interop operations with desired capacity.
             *
             * @param cap Capacity.
             * @return Memory.
             */
            common::concurrent::SharedPointer<interop::InteropMemory> AllocateMemory(int32_t cap);

            /**
             * Get memory chunk located at the given pointer.
             *
             * @param memPtr Memory pointer.
             * @retrun Memory.
             */
            common::concurrent::SharedPointer<interop::InteropMemory> GetMemory(int64_t memPtr);

            /**
             * Get type manager.
             *
             * @return Type manager.
             */
            binary::BinaryTypeManager* GetTypeManager();

            /**
             * Get type updater.
             *
             * @return Type updater.
             */
            binary::BinaryTypeUpdater* GetTypeUpdater();

            /**
             * Notify processor that Ignite instance has started.
             */
            void ProcessorReleaseStart();

            /**
             * Get handle registry.
             *
             * @return Handle registry.
             */
            HandleRegistry& GetHandleRegistry();

        private:
            /** Context to access Java. */
            common::concurrent::SharedPointer<jni::java::JniContext> ctx;

            /** Startup latch. */
            common::concurrent::SingleLatch* latch;

            /** Ignite name. */
            char* name;

            /** Processor instance. */
            jni::JavaGlobalRef proc;

            /** Type manager. */
            binary::BinaryTypeManager* metaMgr;

            /** Type updater. */
            binary::BinaryTypeUpdater* metaUpdater;

            /** Handle registry. */
            HandleRegistry registry;

            IGNITE_NO_COPY_ASSIGNMENT(IgniteEnvironment);
        };
    }
}

#endif //_IGNITE_IMPL_IGNITE_ENVIRONMENT