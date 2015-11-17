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

#ifndef _IGNITE_ODBC_DRIVER_INTEROP_STACK_MEMORY
#define _IGNITE_ODBC_DRIVER_INTEROP_STACK_MEMORY

#include <stdint.h>

#include "ignite/impl/interop/interop_memory.h"

namespace ignite 
{
    namespace odbc
    {
        /**
         * Interop stack memory.
         */
        class InteropStackMemory : public ignite::impl::interop::InteropMemory
        {
        public:
            /**
             * Constructor create new stack memory object from scratch.
             *
             * @param cap Capacity.
             */
            explicit InteropStackMemory(int32_t cap)
            {
                memPtr = mem;

                Data(memPtr, malloc(cap));
                Capacity(memPtr, cap);
                Length(memPtr, 0);
                Flags(memPtr, ignite::impl::interop::IGNITE_MEM_FLAG_EXT);
            }

            /**
             * Destructor.
             */
            ~InteropStackMemory()
            {
                free(Data());
            }

            /**
             * Reallocate memory.
             * @param cap Required capacity.
             */
            virtual void Reallocate(int32_t cap)
            {
                int doubledCap = Capacity() << 1;

                if (doubledCap > cap)
                    cap = doubledCap;

                Data(memPtr, realloc(Data(memPtr), cap));
                Capacity(memPtr, cap);
            }

        private:
            IGNITE_NO_COPY_ASSIGNMENT(InteropStackMemory)

            int8_t mem[ignite::impl::interop::IGNITE_MEM_HDR_LEN];
        };
    }
}

#endif