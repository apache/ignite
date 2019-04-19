/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ignite/ignite_error.h>
#include <ignite/jni/java.h>

#include "ignite/impl/interop/interop_external_memory.h"

using namespace ignite::jni::java;

namespace ignite
{
    namespace impl
    {
        namespace interop
        {
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