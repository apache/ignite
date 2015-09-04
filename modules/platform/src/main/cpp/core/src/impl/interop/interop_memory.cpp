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

#include <ignite/common/java.h>

#include "ignite/impl/interop/interop_memory.h"
#include "ignite/ignite_error.h"

using namespace ignite::common::java;

namespace ignite
{    
    namespace impl
    {
        namespace interop 
        {
            int8_t* InteropMemory::Data(int8_t* memPtr)
            {
                return reinterpret_cast<int8_t*>(*reinterpret_cast<int64_t*>(memPtr));
            }

            void InteropMemory::Data(int8_t* memPtr, void* ptr)
            {
                *reinterpret_cast<int64_t*>(memPtr) = reinterpret_cast<int64_t>(ptr);
            }

            int32_t InteropMemory::Capacity(int8_t* memPtr)
            {
                return *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_CAP);
            }

            void InteropMemory::Capacity(int8_t* memPtr, int32_t val)
            {
                *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_CAP) = val;
            }

            int32_t InteropMemory::Length(int8_t* memPtr)
            {
                return *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_LEN);
            }

            void InteropMemory::Length(int8_t* memPtr, int32_t val)
            {
                *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_LEN) = val;
            }

            int32_t InteropMemory::Flags(int8_t* memPtr)
            {
                return *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_FLAGS);
            }

            void InteropMemory::Flags(int8_t* memPtr, int32_t val)
            {
                *reinterpret_cast<int32_t*>(memPtr + IGNITE_MEM_HDR_OFF_FLAGS) = val;
            }

            bool InteropMemory::IsExternal(int8_t* memPtr)
            {
                return IsExternal(Flags(memPtr));
            }

            bool InteropMemory::IsExternal(int32_t flags)
            {
                return (flags & IGNITE_MEM_FLAG_EXT) != IGNITE_MEM_FLAG_EXT;
            }

            bool InteropMemory::IsPooled(int8_t* memPtr)
            {
                return IsPooled(Flags(memPtr));
            }

            bool InteropMemory::IsPooled(int32_t flags)
            {
                return (flags & IGNITE_MEM_FLAG_POOLED) != 0;
            }

            bool InteropMemory::IsAcquired(int8_t* memPtr)
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

            int32_t InteropMemory::Capacity()
            {
                return Capacity(memPtr);
            }

            int32_t InteropMemory::Length()
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

            InteropExternalMemory::InteropExternalMemory(int8_t* memPtr) 
            {
                this->memPtr = memPtr;
            }

            void InteropExternalMemory::Reallocate(int32_t cap)
            {
                if (JniContext::Reallocate(reinterpret_cast<int64_t>(memPtr), cap) == -1) {
                    IGNITE_ERROR_FORMATTED_2(IgniteError::IGNITE_ERR_MEMORY, "Failed to reallocate external memory", 
                        "memPtr", PointerLong(), "requestedCapacity", cap)
                }
            }
        }
    }
}