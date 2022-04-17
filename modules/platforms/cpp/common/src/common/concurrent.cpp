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

#include "ignite/common/concurrent.h"

namespace ignite
{
    namespace common
    {
        namespace concurrent
        {
            /** Thread-local index generator for application. */
            int32_t appTlsIdxGen = 0;

            int32_t ThreadLocal::NextIndex()
            {
                return Atomics::IncrementAndGet32(&appTlsIdxGen);
            }

            void ThreadLocal::Remove(int32_t idx)
            {
                void* val = Get0();

                if (val)
                {
                    std::map<int32_t, ThreadLocalEntry*>* map =
                        static_cast<std::map<int32_t, ThreadLocalEntry*>*>(val);

                    ThreadLocalEntry* appVal = (*map)[idx];

                    if (appVal)
                        delete appVal;

                    map->erase(idx);

                    if (map->size() == 0)
                    {
                        delete map;

                        Set0(NULL);
                    }
                }
            }

            void ThreadLocal::Clear0(void* mapPtr)
            {
                if (mapPtr)
                {
                    std::map<int32_t, ThreadLocalEntry*>* map =
                        static_cast<std::map<int32_t, ThreadLocalEntry*>*>(mapPtr);

                    for (std::map<int32_t, ThreadLocalEntry*>::iterator it = map->begin(); it != map->end(); ++it)
                        delete it->second;

                    delete map;
                }
            }

            SharedPointerImpl::SharedPointerImpl(void* ptr, DeleterType deleter) :
                ptr(ptr), deleter(deleter), refCnt(1)
            {
                Memory::Fence();
            }

            void* SharedPointerImpl::Pointer()
            {
                return ptr;
            }

            const void* SharedPointerImpl::Pointer() const
            {
                return ptr;
            }

            SharedPointerImpl::DeleterType SharedPointerImpl::Deleter()
            {
                return deleter;
            }

            void SharedPointerImpl::Increment()
            {
                Atomics::IncrementAndGet32(&refCnt);
            }

            bool SharedPointerImpl::Decrement()
            {
                return Atomics::DecrementAndGet32(&refCnt) == 0;
            }
        }
    }
}
