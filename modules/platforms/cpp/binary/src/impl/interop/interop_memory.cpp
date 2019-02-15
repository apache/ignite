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
#include <cstdlib>
#include <ignite/ignite_error.h>

#include "ignite/impl/interop/interop_memory.h"

namespace ignite
{    
    namespace impl
    {
        namespace interop 
        {
            int8_t* InteropMemory::Data(const int8_t* memPtr)
            {
                return reinterpret_cast<int8_t*>(*reinterpret_cast<const int64_t*>(memPtr));
            }

            void InteropMemory::Data(int8_t* memPtr, void* ptr)
            {
                *reinterpret_cast<int64_t*>(memPtr) = reinterpret_cast<int64_t>(ptr);
            }

            int32_t InteropMemory::Capacity(const int8_t* memPtr)
            {
                return *reinterpret_cast<const int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_CAP);
            }

            void InteropMemory::Capacity(int8_t* memPtr, int32_t val)
            {
                *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_CAP) = val;
            }

            int32_t InteropMemory::Length(const int8_t* memPtr)
            {
                return *reinterpret_cast<const int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_LEN);
            }

            void InteropMemory::Length(int8_t* memPtr, int32_t val)
            {
                *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_LEN) = val;
            }

            int32_t InteropMemory::Flags(const int8_t* memPtr)
            {
                return *reinterpret_cast<const int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_FLAGS);
            }

            void InteropMemory::Flags(int8_t* memPtr, int32_t val)
            {
                *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_FLAGS) = val;
            }

            bool InteropMemory::IsExternal(const int8_t* memPtr)
            {
                return IsExternal(Flags(memPtr));
            }

            bool InteropMemory::IsExternal(int32_t flags)
            {
                return (flags & IGNITE_MEM_FLAG_EXT) != IGNITE_MEM_FLAG_EXT;
            }

            bool InteropMemory::IsPooled(const int8_t* memPtr)
            {
                return IsPooled(Flags(memPtr));
            }

            bool InteropMemory::IsPooled(int32_t flags)
            {
                return (flags & IGNITE_MEM_FLAG_POOLED) != 0;
            }

            bool InteropMemory::IsAcquired(const int8_t* memPtr)
            {
                return IsAcquired(Flags(memPtr));
            }

            bool InteropMemory::IsAcquired(int32_t flags)
            {
                return (flags & IGNITE_MEM_FLAG_ACQUIRED) != 0;
            }
                
            int8_t* InteropMemory::Pointer()
            {
                return memPtr;
            }

            int64_t InteropMemory::PointerLong()
            {
                return reinterpret_cast<int64_t>(memPtr);
            }

            int8_t* InteropMemory::Data()
            {
                return Data(memPtr);
            }

            const int8_t* InteropMemory::Data() const
            {
                return Data(memPtr);
            }

            int32_t InteropMemory::Capacity() const
            {
                return Capacity(memPtr);
            }

            void InteropMemory::Capacity(int32_t val)
            {
                Capacity(memPtr, val);
            }

            int32_t InteropMemory::Length() const
            {
                return Length(memPtr);
            }

            void InteropMemory::Length(int32_t val)
            {
                Length(memPtr, val);
            }
                
            InteropUnpooledMemory::InteropUnpooledMemory(int32_t cap)
            {
                memPtr = static_cast<int8_t*>(malloc(IGNITE_MEM_HDR_LEN));
                
                Data(memPtr, malloc(cap));
                Capacity(memPtr, cap);
                Length(memPtr, 0);
                Flags(memPtr, IGNITE_MEM_FLAG_EXT);

                owning = true;
            }

            InteropUnpooledMemory::InteropUnpooledMemory(int8_t* memPtr)
            {
                this->memPtr = memPtr;
                this->owning = false;
            }

            InteropUnpooledMemory::~InteropUnpooledMemory()
            {
                if (owning) {
                    free(Data());
                    free(memPtr);
                }
            }

            void InteropUnpooledMemory::Reallocate(int32_t cap)
            {
                int doubledCap = Capacity() << 1;

                if (doubledCap > cap)
                    cap = doubledCap;

                Data(memPtr, realloc(Data(memPtr), cap));
                Capacity(memPtr, cap);
            }
        }
    }
}