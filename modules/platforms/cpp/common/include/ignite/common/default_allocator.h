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