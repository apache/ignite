/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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