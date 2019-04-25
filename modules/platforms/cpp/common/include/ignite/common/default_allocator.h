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
#ifndef _IGNITE_COMMON_DEFAULT_ALLOCATOR
#define _IGNITE_COMMON_DEFAULT_ALLOCATOR

#include <stdint.h>
#include <cassert>

#include <ignite/common/common.h>

namespace ignite
{
    namespace common
    {
        /**
         * Allocator. Manages objects construction and destruction as well
         * as a memory allocation.
         */
        template<typename T>
        class IGNITE_IMPORT_EXPORT DefaultAllocator
        {
        public:
            typedef T ValueType;
            typedef T* PointerType;
            typedef T& ReferenceType;
            typedef const T* ConstPointerType;
            typedef const T& ConstReferenceType;
            typedef int32_t SizeType;
            typedef int32_t DifferenceType;

            template <class T2> struct Rebind
            {
                typedef DefaultAllocator<T2> other;
            };

            /**
             * Default constructor.
             */
            DefaultAllocator()
            {
                // No-op.
            }

            /**
             * Destructor.
             */
            ~DefaultAllocator()
            {
                // No-op.
            }

            PointerType Allocate(SizeType len, void* hint = 0)
            {
                return static_cast<PointerType>(::operator new(len * sizeof(ValueType)));
            }

            void Deallocate(PointerType ptr, SizeType len)
            {
                ::operator delete(ptr);
            }

            void Construct(PointerType p, ConstReferenceType val)
            {
                new (p) ValueType(val);
            }

            void Destruct(PointerType p)
            {
                p->~ValueType();
            }
        };
    }
}

#endif // _IGNITE_COMMON_DEFAULT_ALLOCATOR