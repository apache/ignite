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
#ifndef _IGNITE_COMMON_DYNAMIC_SIZE_ARRAY
#define _IGNITE_COMMON_DYNAMIC_SIZE_ARRAY

#include <stdint.h>
#include <cstring>
#include <cassert>

#include <utility>

#include <ignite/common/common.h>
#include <ignite/common/default_allocator.h>
#include <ignite/common/bits.h>

namespace ignite
{
    namespace common
    {
        /**
         * Dynamic size array is safe container abstraction with a dynamic size.
         * This is the analogue of the standard vector. It is needed to be used
         * in exported classes as we can't export standard library classes.
         */
        template<typename T, typename A = DefaultAllocator<T> >
        class IGNITE_IMPORT_EXPORT DynamicSizeArray
        {
        public:
            typedef T ValueType;
            typedef A AllocatorType;
            typedef typename AllocatorType::SizeType SizeType;
            typedef typename AllocatorType::PointerType PointerType;
            typedef typename AllocatorType::ConstPointerType ConstPointerType;
            typedef typename AllocatorType::ReferenceType ReferenceType;
            typedef typename AllocatorType::ConstReferenceType ConstReferenceType;

            /**
             * Default constructor.
             *
             * Constructs zero-size and zero-capasity array.
             */
            DynamicSizeArray(const AllocatorType& allocator = AllocatorType()) :
                alloc(allocator),
                size(0),
                capasity(0),
                data(0)
            {
                // No-op.
            }

            /**
             * Constructor.
             * Constructs empty array with the specified capacity.
             *
             * @param len Array length.
             * @param alloc Allocator.
             */
            DynamicSizeArray(SizeType len, const AllocatorType& allocator = AllocatorType()) :
                alloc(allocator),
                size(0),
                capasity(bits::GetCapasityForSize(len)),
                data(alloc.Allocate(capasity))
            {
                // No-op.
            }

            /**
             * Raw array constructor.
             *
             * @param arr Raw array.
             * @param len Array length in elements.
             */
            DynamicSizeArray(ConstPointerType arr, SizeType len,
                const AllocatorType& allocator = AllocatorType()) :
                alloc(allocator),
                size(0),
                capasity(0),
                data(0)
            {
                Assign(arr, len);
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            DynamicSizeArray(const DynamicSizeArray<T>& other) :
                alloc(),
                size(0),
                capasity(0),
                data(0)
            {
                Assign(other);
            }

            /**
             * Destructor.
             */
            ~DynamicSizeArray()
            {
                for (PointerType it = data; it != data + size; ++it)
                    alloc.Destruct(it);

                alloc.Deallocate(data, capasity);
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return Reference to this instance.
             */
            DynamicSizeArray<T>& operator=(const DynamicSizeArray<T>& other)
            {
                Assign(other);

                return *this;
            }

            /**
             * Assign new value to the array.
             *
             * @param other Another array instance.
             */
            void Assign(const DynamicSizeArray<T>& other)
            {
                if (this != &other)
                {
                    alloc = other.alloc;

                    Assign(other.GetData(), other.GetSize());
                }
            }

            /**
             * Assign new value to the array.
             *
             * @param src Raw array.
             * @param len Array length in elements.
             */
            void Assign(ConstPointerType src, SizeType len)
            {
                for (PointerType it = data; it != data + size; ++it)
                    alloc.Destruct(it);

                if (capasity < len)
                {
                    alloc.Deallocate(data, capasity);

                    capasity = bits::GetCapasityForSize(len);
                    data = alloc.Allocate(capasity);
                }

                size = len;

                for (SizeType i = 0; i < size; ++i)
                    alloc.Construct(data + i, src[i]);
            }

            /**
             * Append several values to the array.
             *
             * @param src Raw array.
             * @param len Array length in elements.
             */
            void Append(ConstPointerType src, SizeType len)
            {
                Reserve(size + len);

                for (SizeType i = 0; i < len; ++i)
                    alloc.Construct(data + size + i, src[i]);

                size += len;
            }

            /**
             * Swap contents of the array with another instance.
             *
             * @param other Instance to swap with.
             */
            void Swap(DynamicSizeArray<T>& other)
            {
                if (this != &other)
                {
                    std::swap(alloc, other.alloc);
                    std::swap(size, other.size);
                    std::swap(capasity, other.capasity);
                    std::swap(data, other.data);
                }
            }

            /**
             * Get data pointer.
             *
             * @return Data pointer.
             */
            PointerType GetData()
            {
                return data;
            }

            /**
             * Get data pointer.
             *
             * @return Data pointer.
             */
            ConstPointerType GetData() const
            {
                return data;
            }

            /**
             * Get array size.
             *
             * @return Array size.
             */
            SizeType GetSize() const
            {
                return size;
            }

            /**
             * Get capasity.
             *
             * @return Array capasity.
             */
            SizeType GetCapasity() const
            {
                return capasity;
            }

            /**
             * Element access operator.
             *
             * @param idx Element index.
             * @return Element reference.
             */
            ReferenceType operator[](SizeType idx)
            {
                assert(idx < size);

                return data[idx];
            }

            /**
             * Element access operator.
             *
             * @param idx Element index.
             * @return Element reference.
             */
            ConstReferenceType operator[](SizeType idx) const
            {
                assert(idx < size);

                return data[idx];
            }

            /**
             * Check if the array is empty.
             *
             * @return True if the array is empty.
             */
            bool IsEmpty() const
            {
                return size == 0;
            }

            /**
             * Clears the array.
             */
            void Clear()
            {
                for (PointerType it = data; it != data + size; ++it)
                    alloc.Destruct(it);

                size = 0;
            }

            /**
             * Reserves not less than specified elements number so array is not
             * going to grow on append.
             *
             * @param newCapacity Desired capasity.
             */
            void Reserve(SizeType newCapacity)
            {
                if (capasity < newCapacity)
                {
                    DynamicSizeArray<T> tmp(newCapacity);

                    tmp.Assign(*this);

                    Swap(tmp);
                }
            }

            /**
             * Resizes array. Destructs elements if the specified size is less
             * than the array's size. Default-constructs elements if the
             * specified size is more than the array's size.
             *
             * @param newSize Desired size.
             */
            void Resize(SizeType newSize)
            {
                if (capasity < newSize)
                    Reserve(newSize);

                if (newSize > size)
                {
                    for (PointerType it = data + size; it < data + newSize; ++it)
                        alloc.Construct(it, ValueType());
                }
                else
                {
                    for (PointerType it = data + newSize; it < data + size; ++it)
                        alloc.Destruct(it);
                }

                size = newSize;
            }

            /**
             * Get last element.
             *
             * @return Last element reference.
             */
            const ValueType& Back() const
            {
                assert(size > 0);

                return data[size - 1];
            }

            /**
             * Get last element.
             *
             * @return Last element reference.
             */
            ValueType& Back()
            {
                assert(size > 0);

                return data[size - 1];
            }

            /**
             * Get first element.
             *
             * @return First element reference.
             */
            const ValueType& Front() const
            {
                assert(size > 0);

                return data[0];
            }

            /**
             * Get first element.
             *
             * @return First element reference.
             */
            ValueType& Front()
            {
                assert(size > 0);

                return data[0];
            }

            /**
             * Pushes new value to the back of the array, effectively increasing
             * array size by one.
             *
             * @param val Value to push.
             */
            void PushBack(ConstReferenceType val)
            {
                Resize(size + 1);

                Back() = val;
            }

        private:
            /** Allocator */
            AllocatorType alloc;

            /** Array size. */
            SizeType size;

            /** Array capasity. */
            SizeType capasity;

            /** Data. */
            PointerType data;
        };
    }
}

#endif // _IGNITE_COMMON_DYNAMIC_SIZE_ARRAY