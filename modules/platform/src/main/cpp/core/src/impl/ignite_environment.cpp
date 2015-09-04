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

#include "ignite/impl/portable/portable_reader_impl.h"
#include "ignite/impl/ignite_environment.h"
#include "ignite/portable/portable.h"

using namespace ignite::common::concurrent;
using namespace ignite::common::java;
using namespace ignite::impl::interop;
using namespace ignite::impl::portable;
using namespace ignite::portable;

namespace ignite 
{
    namespace impl 
    {
        /**
         * OnStart callback.
         *
         * @param target Target environment.
         * @param memPtr Memory pointer.
         */
        void IGNITE_CALL OnStart(void* target, long long memPtr)
        {
            SharedPointer<IgniteEnvironment>* ptr = static_cast<SharedPointer<IgniteEnvironment>*>(target);

            ptr->Get()->OnStartCallback(memPtr);
        }

        /**
         * OnStop callback.
         *
         * @param target Target environment.
         */
        void IGNITE_CALL OnStop(void* target)
        {
            SharedPointer<IgniteEnvironment>* ptr = static_cast<SharedPointer<IgniteEnvironment>*>(target);

            delete ptr;
        } 

        IgniteEnvironment::IgniteEnvironment() : ctx(SharedPointer<JniContext>()), latch(new SingleLatch), name(NULL),
            metaMgr(new PortableMetadataManager())
        {
            // No-op.
        }

        IgniteEnvironment::~IgniteEnvironment()
        {
            delete latch;

            if (name)
                delete name;

            delete metaMgr;
        }

        JniHandlers IgniteEnvironment::GetJniHandlers(SharedPointer<IgniteEnvironment>* target)
        {
            JniHandlers hnds = JniHandlers();

            hnds.target = target;

            hnds.onStart = OnStart;
            hnds.onStop = OnStop;

            hnds.error = NULL;

            return hnds;
        }
            
        void IgniteEnvironment::Initialize(SharedPointer<JniContext> ctx)
        {
            this->ctx = ctx;
                
            latch->CountDown();
        }
        
        char* IgniteEnvironment::InstanceName()
        {
            return name;
        }

        JniContext* IgniteEnvironment::Context()
        {
            return ctx.Get();
        }

        SharedPointer<InteropMemory> IgniteEnvironment::AllocateMemory()
        {
            SharedPointer<InteropMemory> ptr(new InteropUnpooledMemory(1024));

            return ptr;
        }

        SharedPointer<InteropMemory> IgniteEnvironment::AllocateMemory(int32_t cap)
        {
            SharedPointer<InteropMemory> ptr(new InteropUnpooledMemory(cap));

            return ptr;
        }

        SharedPointer<InteropMemory> IgniteEnvironment::GetMemory(int64_t memPtr)
        {
            int8_t* memPtr0 = reinterpret_cast<int8_t*>(memPtr);

            int32_t flags = InteropMemory::Flags(memPtr0);

            if (InteropMemory::IsExternal(flags))
            {
                SharedPointer<InteropMemory> ptr(new InteropExternalMemory(memPtr0));

                return ptr;
            }
            else
            {
                SharedPointer<InteropMemory> ptr(new InteropUnpooledMemory(memPtr0));

                return ptr;
            }
        }

        PortableMetadataManager* IgniteEnvironment::GetMetadataManager()
        {
            return metaMgr;
        }

        void IgniteEnvironment::OnStartCallback(long long memPtr)
        {
            InteropExternalMemory mem(reinterpret_cast<int8_t*>(memPtr));
            InteropInputStream stream(&mem);

            PortableReaderImpl reader(&stream);
            
            int32_t nameLen = reader.ReadString(NULL, 0);

            if (nameLen >= 0)
            {
                name = new char[nameLen + 1];
                reader.ReadString(name, nameLen + 1);
            }
            else
                name = NULL;
        }
    }
}





