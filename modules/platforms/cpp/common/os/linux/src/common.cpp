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
#include <pthread.h>

#include "ignite/common/common.h"
#include "ignite/common/java.h"

using namespace ignite::common::java;

namespace ignite
{
    namespace common
    {
        /** Key indicating that the thread is attached. */
        static pthread_key_t attachKey;

        /** Helper to ensure that attach key is allocated only once. */
        static pthread_once_t attachKeyInit = PTHREAD_ONCE_INIT;
        
        AttachHelper::~AttachHelper()
        {
            JniContext::Detach();
        }
        
        void AttachHelper::OnThreadAttach()
        {
            pthread_once(&attachKeyInit, AllocateAttachKey);
            
            void* val = pthread_getspecific(attachKey);
            
            if (!val)
                pthread_setspecific(attachKey, new AttachHelper());
        }
        
        void AttachHelper::AllocateAttachKey()
        {
            pthread_key_create(&attachKey, DestroyAttachKey);
        }   
        
        void AttachHelper::DestroyAttachKey(void* key)
        {
            delete reinterpret_cast<AttachHelper*>(key);
        }             
    }
}
