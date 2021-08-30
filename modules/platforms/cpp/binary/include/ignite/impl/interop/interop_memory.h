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

#ifndef _IGNITE_IMPL_INTEROP_INTEROP_MEMORY
#define _IGNITE_IMPL_INTEROP_INTEROP_MEMORY

#include <stdint.h>

#include <ignite/common/common.h>

namespace ignite 
{
    namespace impl 
    {
        namespace interop 
        {
            /** Memory header length. */
            const int IGNITE_MEM_HDR_LEN = 20;

            /** Memory header offset: capacity. */
            const int IGNITE_MEM_HDR_OFF_CAP = 8;

            /** Memory header offset: length. */
            const int IGNITE_MEM_HDR_OFF_LEN = 12;

            /** Memory header offset: flags. */
            const int IGNITE_MEM_HDR_OFF_FLAGS = 16;

            /** Flag: external. */
            const int IGNITE_MEM_FLAG_EXT = 0x1;

            /** Flag: pooled. */
            const int IGNITE_MEM_FLAG_POOLED = 0x2;

            /** Flag: acquired. */
            const int IGNITE_MEM_FLAG_ACQUIRED = 0x4;

            /**
             * A helper union to bitwise conversion from int32_t to float and back.
             */
            union BinaryFloatInt32
            {
                float f;
                int32_t i;
            };

            /**
             * A helper union to bitwise conversion from int64_t to double and back.
             */
            union BinaryDoubleInt64
            {
                double d;
                int64_t i;
            };

            /**
             * Interop memory.
             */
            class IGNITE_IMPORT_EXPORT InteropMemory
            {
            public:
                /**
                 * Get raw data pointer.
                 *
                 * @param memPtr Memory pointer.
                 * @return Raw data pointer.
                 */
                static int8_t* Data(const int8_t* memPtr);

                /**
                 * Set raw data pointer.
                 *
                 * @param memPtr Memory pointer.
                 * @param ptr Raw data pointer.
                 */
                static void Data(int8_t* memPtr, void* ptr);

                /**
                 * Get capacity.
                 *
                 * @param memPtr Memory pointer.
                 * @return Capacity.
                 */
                static int32_t Capacity(const int8_t* memPtr);

                /**
                 * Set capacity.
                 *
                 * @param memPtr Memory pointer.
                 * @param val Value.
                 */
                static void Capacity(int8_t* memPtr, int32_t val);

                /**
                 * Get length.
                 *
                 * @param memPtr Memory pointer.
                 * @return Length.
                 */
                static int32_t Length(const int8_t* memPtr);

                /**
                 * Set length.
                 *
                 * @param memPtr Memory pointer.
                 * @param val Value.
                 */
                static void Length(int8_t* memPtr, int32_t val);

                /**
                 * Get flags.
                 *
                 * @param memPtr Memory pointer.
                 * @return Flags.
                 */
                static int32_t Flags(const int8_t* memPtr);

                /**
                 * Set flags.
                 *
                 * @param memPtr Memory pointer.
                 * @param val Value.
                 */
                static void Flags(int8_t* memPtr, int32_t val);

                /**
                 * Get "external" flag state.
                 *
                 * @param memPtr Memory pointer.
                 * @return Flag state.
                 */
                static bool IsExternal(const int8_t* memPtr);

                /**
                 * Get "external" flag state.
                 *
                 * @param flags Flags.
                 * @return Flag state.
                 */
                static bool IsExternal(int32_t flags);

                /**
                 * Get "pooled" flag state.
                 *
                 * @param memPtr Memory pointer.
                 * @return Flag state.
                 */
                static bool IsPooled(const int8_t* memPtr);

                /**
                 * Get "pooled" flag state.
                 *
                 * @param flags Flags.
                 * @return Flag state.
                 */
                static bool IsPooled(int32_t flags);

                /**
                 * Get "acquired" flag state.
                 *
                 * @param memPtr Memory pointer.
                 * @return Flag state.
                 */
                static bool IsAcquired(const int8_t* memPtr);

                /**
                 * Get "acquired" flag state.
                 *
                 * @param flags Flags.
                 * @return Flag state.
                 */
                static bool IsAcquired(int32_t flags);

                /**
                 * Constructor.
                 */
                InteropMemory() : memPtr(0) { }

                /**
                 * Destructor.
                 */
                virtual ~InteropMemory() { }
                    
                /**
                 * Get cross-platform memory pointer.
                 *
                 * @return Memory pointer.
                 */
                int8_t* Pointer();

                /**
                 * Get cross-platform pointer in long form.
                 */
                int64_t PointerLong();

                /**
                 * Get raw data pointer.
                 *
                 * @return Data pointer.
                 */
                int8_t* Data();

                /**
                 * Get raw data pointer.
                 *
                 * @return Data pointer.
                 */
                const int8_t* Data() const;

                /**
                 * Get capacity.
                 *
                 * @return Capacity.
                 */
                int32_t Capacity() const;

                /**
                * Set capacity.
                *
                * @param val Capacity.
                */
                void Capacity(int32_t val);

                /**
                 * Get length.
                 *
                 * @return Length.
                 */
                int32_t Length() const;

                /**
                 * Set length.
                 *
                 * @param val Length.
                 */
                void Length(int32_t val);

                /**
                 * Reallocate memory.
                 *
                 * @param cap Desired capacity.
                 */
                virtual void Reallocate(int32_t cap) = 0;
            protected:
                /** Memory pointer. */
                int8_t* memPtr; 
            };

            /**
             * Interop unpooled memory.
             */
            class IGNITE_IMPORT_EXPORT InteropUnpooledMemory : public InteropMemory
            {
            public:
                /**
                 * Constructor create new unpooled memory object from scratch.
                 *
                 * @param cap Capacity.
                 */
                explicit InteropUnpooledMemory(int32_t cap);

                /**
                 * Constructor creating unpooled memory object from existing memory pointer.
                 *
                 * @param memPtr Memory pointer.
                 */
                explicit InteropUnpooledMemory(int8_t* memPtr = 0);

                /**
                 * Destructor.
                 */
                ~InteropUnpooledMemory();

                virtual void Reallocate(int32_t cap);

                /**
                 * Try get owning copy.
                 *
                 * @param mem Memory instance to transfer ownership to.
                 * @return True on success
                 */
                bool TryGetOwnership(InteropUnpooledMemory& mem);

            private:
                /**
                 * Release all resources.
                 */
                void CleanUp();

                /** Whether this instance is owner of memory chunk. */
                bool owning; 

                IGNITE_NO_COPY_ASSIGNMENT(InteropUnpooledMemory);
            };
        }
    }
}

#endif //_IGNITE_IMPL_INTEROP_INTEROP_MEMORY
