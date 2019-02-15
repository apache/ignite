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

/**
 * @file
 * Declares ignite::ReferenceImplBase class and its implementations.
 */

#ifndef _IGNITE_COMMON_REFERENCE_IMPL
#define _IGNITE_COMMON_REFERENCE_IMPL

#include <utility>

#include <ignite/common/common.h>

namespace ignite
{
    namespace common
    {
        // Any number is good as long as it is not null.
        enum { POINTER_CAST_MAGIC_NUMBER = 80000 };

        /**
         * Interface for constant Reference implementation class template.
         */
        class ConstReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ConstReferenceImplBase()
            {
                // No-op.
            }

            /**
             * Get the constant pointer.
             *
             * @return Constant pointer to underlying value.
             */
            virtual const void* Get() const = 0;
        };

        /**
         * Interface for Reference implementation class template.
         */
        class ReferenceImplBase : public ConstReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ReferenceImplBase()
            {
                // No-op.
            }

            virtual const void* Get() const = 0;

            /**
             * Get the pointer.
             *
             * @return Pointer to underlying value.
             */
            virtual void* Get() = 0;
        };

        /**
         * Reference class implementation for smart pointers.
         *
         * Note, this class does not implement any smart pointer functionality
         * itself, instead it wraps one of the existing wide-spread smart
         * pointer implementations and provides unified interface for them.
         */
        template<typename P>
        class ReferenceSmartPointer : public ReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ReferenceSmartPointer()
            {
                // No-op.
            }

            /**
             * Default constructor.
             */
            ReferenceSmartPointer() :
                ptr()
            {
                // No-op.
            }

            virtual const void* Get() const
            {
                return reinterpret_cast<const void*>(&(*ptr));
            }

            virtual void* Get()
            {
                return reinterpret_cast<void*>(&(*ptr));
            }

            /**
             * Swap underlying smart pointer.
             *
             * @param other Another instance.
             */
            void Swap(P& other)
            {
                using std::swap;

                swap(ptr, other);
            }

        private:
            /** Underlying pointer. */
            P ptr;
        };

        /**
         * Reference implementation for the owning raw pointer.
         */
        template<typename T>
        class ReferenceOwningRawPointer : public ReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ReferenceOwningRawPointer()
            {
                delete ptr;
            }

            /**
             * Default constructor.
             */
            ReferenceOwningRawPointer() :
                ptr(0)
            {
                // No-op.
            }

            /**
             * Pointer constructor.
             *
             * @param ptr Pointer to take ownership over.
             */
            ReferenceOwningRawPointer(T* ptr) :
                ptr(ptr)
            {
                // No-op.
            }

            virtual const void* Get() const
            {
                return reinterpret_cast<const void*>(ptr);
            }

            virtual void* Get()
            {
                return reinterpret_cast<void*>(ptr);
            }

        private:
            /** Underlying pointer. */
            T* ptr;
        };

        /**
         * Reference implementation for the raw pointer.
         */
        template<typename T>
        class ReferenceNonOwningRawPointer : public ReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ReferenceNonOwningRawPointer()
            {
                // No-op.
            }

            /**
             * Default constructor.
             */
            ReferenceNonOwningRawPointer() :
                ptr(0)
            {
                // No-op.
            }

            /**
             * Pointer constructor.
             *
             * @param ptr Pointer.
             */
            ReferenceNonOwningRawPointer(T* ptr) :
                ptr(ptr)
            {
                // No-op.
            }

            virtual const void* Get() const
            {
                return reinterpret_cast<const void*>(ptr);
            }

            virtual void* Get()
            {
                return reinterpret_cast<void*>(ptr);
            }

        private:
            /** Underlying pointer. */
            T* ptr;
        };

        /**
         * Constant reference implementation for the raw pointer.
         */
        template<typename T>
        class ConstReferenceNonOwningRawPointer : public ConstReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~ConstReferenceNonOwningRawPointer()
            {
                // No-op.
            }

            /**
             * Default constructor.
             */
            ConstReferenceNonOwningRawPointer() :
                ptr(0)
            {
                // No-op.
            }

            /**
             * Pointer constructor.
             *
             * @param ptr Pointer.
             */
            ConstReferenceNonOwningRawPointer(const T* ptr) :
                ptr(ptr)
            {
                // No-op.
            }

            virtual const void* Get() const
            {
                return reinterpret_cast<const void*>(ptr);
            }

        private:
            /** Underlying pointer. */
            const T* ptr;
        };

    }
}

#endif //_IGNITE_COMMON_REFERENCE_IMPL