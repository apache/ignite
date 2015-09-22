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

#ifndef _IGNITE_ENVIRONMENT
#define _IGNITE_ENVIRONMENT

#include <ignite/common/concurrent.h>
#include <ignite/common/java.h>

#include "ignite/impl/interop/interop_memory.h"
#include "portable/portable_metadata_manager.h"

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
             * @param Target (current env wrapped into a shared pointer).
             * @return JNI handlers.
             */
            ignite::common::java::JniHandlers GetJniHandlers(ignite::common::concurrent::SharedPointer<IgniteEnvironment>* target);
                
            /**
             * Perform initialization on successful start.
             *
             * @param ctx Context.
             */
            void Initialize(ignite::common::concurrent::SharedPointer<ignite::common::java::JniContext> ctx);

            /**
             * Start callback.
             *
             * @param memPtr Memory pointer.
             */
            void OnStartCallback(long long memPtr);        
            
            /**
             * Get name of Ignite instance.
             *
             * @return Name.
             */
            char* InstanceName();

            /**
             * Get JNI context.
             *
             * @return Context.
             */
            ignite::common::java::JniContext* Context();

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
             * Get metadata manager.
             *
             * @param Metadata manager.
             */
            portable::PortableMetadataManager* GetMetadataManager();
        private:
            /** Context to access Java. */
            ignite::common::concurrent::SharedPointer<ignite::common::java::JniContext> ctx;

            /** Startup latch. */
            ignite::common::concurrent::SingleLatch* latch;

            /** Ignite name. */
            char* name;

            /** Metadata manager. */
            portable::PortableMetadataManager* metaMgr;       

            IGNITE_NO_COPY_ASSIGNMENT(IgniteEnvironment);
        };
    }    
}

#endif