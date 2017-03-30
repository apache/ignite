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
#ifndef _IGNITE_COMMON_FIXED_SIZE_ARRAY
#define _IGNITE_COMMON_FIXED_SIZE_ARRAY

#include <stdint.h>
#include <cstring>
#include <cassert>

#include <utility>
#include <algorithm>

#include <ignite/common/common.h>

namespace ignite
{
    namespace common
    {
        /**
         * Fixed size array is safe array abstraction with a fixed size.
         * The size can be set during runtime though once array is created
         * its size can not be changed without resetting arrays content.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT FixedSizeArray
        {
        public:
            typedef int32_t SizeType;

            /**
             * Default constructor.
             *
             * Constructs zero-size array.
             */
            FixedSizeArray() :
                size(0),
                data(0)
            {
                // No-op.
            }

            /**
             * Constructor.
             * Constructs default-initialized array of the specified length.
             * Array zeroed if T is a POD type.
             *
             * @param len Array length.
             */
            FixedSizeArray(SizeType len) :
                size(len),
                // Brackets are here for a purpose - this way allocated
                // array is zeroed if T is POD type.
                data(new T[size]())
            {
                // No-op.
            }

            /**
             * Copy constructor.
             *
             * @param other Other instance.
             */
            FixedSizeArray(const FixedSizeArray<T>& other) :
                size(other.size),
                data(new T[size])
            {
                Assign(other);
            }

            /**
             * Raw array constructor.
             *
             * @param arr Raw array.
             * @param len Array length in elements.
             */
            FixedSizeArray(const T* arr, SizeType len) :
                size(len),
                data(new T[size])
            {
                Assign(arr, len);
            }

            /**
             * Assignment operator.
             *
             * @param other Other instance.
             * @return Reference to this instance.
             */
            FixedSizeArray<T>& operator=(const FixedSizeArray<T>& other)
            {
                Assign(other);

                return *this;
            }

            /**
             * Assign new value to the array.
             *
             * @param other Another array instance.
             */
            void Assign(const FixedSizeArray<T>& other)
            {
                if (this != &other)
                    Assign(other.GetData(), other.GetSize());
            }

            /**
             * Assign new value to the array.
             *
             * @param src Raw array.
             * @param len Array length in elements.
             */
            void Assign(const T* src, SizeType len)
            {
                // In case we would not need to clean anything
                // its okay to call delete[] on 0.
                T* toClean = 0;

                if (len != size)
                {
                    // Do not clean just yet in case the part of the
                    // array is being assigned to the array.
                    toClean = data;

                    size = len;
                    data = new T[size];
                }

                for (SizeType i = 0; i < len; ++i)
                    data[i] = src[i];

                delete[] toClean;
            }

            /**
             * Swap contents of the array with another instance.
             *
             * @param other Instance to swap with.
             */
            void Swap(FixedSizeArray<T>& other)
            {
                if (this != &other)
                {
                    std::swap(size, other.size);
                    std::swap(data, other.data);
                }
            }

            /**
             * Destructor.
             */
            ~FixedSizeArray()
            {
                // Not a bug. Delete works just fine on null pointers.
                delete[] data;
            }

            /**
             * Get data pointer.
             *
             * @return Data pointer.
             */
            T* GetData()
            {
                return data;
            }

            /**
             * Get data pointer.
             *
             * @return Data pointer.
             */
            const T* GetData() const
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
             * Copy part of the array and place in another array.
             * Contents of the provided array gets swapped with the copy of the
             * specified array part.
             *
             * @param pos Start position.
             * @param n Number of elements to copy.
             * @param result Instance of an array where result should be placed.
             */
            void CopyPart(SizeType pos, SizeType n, FixedSizeArray<T>& result) const
            {
                assert(pos < size);
                assert(pos + n <= size);

                result.Assign(data + pos, n);
            }

            /**
             * Element access operator.
             *
             * @param idx Element index.
             * @return Element reference.
             */
            T& operator[](SizeType idx)
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
            const T& operator[](SizeType idx) const
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
             * Resets the state of the array setting it to the specified size
             * and erasing its content.
             *
             * @param newSize New array size.
             */
            void Reset(SizeType newSize = 0)
            {
                if (size != newSize)
                {
                    delete[] data;

                    if (newSize)
                        data = new T[newSize]();
                    else
                        data = 0;

                    size = newSize;
                }
                else
                    std::fill(data, data + size, T());
            }

        private:
            /** Array size. */
            SizeType size;

            /** Target array. */
            T* data;
        };
    }
}

#endif // _IGNITE_COMMON_FIXED_SIZE_ARRAY