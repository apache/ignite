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

/**
 * @file
 * Declares ignite::AnyReference class.
 */

#ifndef _IGNITE_COMMON_SMART_POINTER
#define _IGNITE_COMMON_SMART_POINTER

#include <cstddef>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace common
    {
        /**
         * Smart pointer interface.
         */
        class AnyReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AnyReferenceImplBase()
            {
                // No-op.
            }

            /**
             * Get the pointer.
             *
             * @return Constant pointer to underlying value.
             */
            virtual const void* Get() const = 0;

            /**
             * Get the pointer.
             *
             * @return Pointer to underlying value.
             */
            virtual void* Get() = 0;
        };

        /**
         * Smart pointer implementation class.
         *
         * Note, this class does not implement any smart pointer functionality
         * itself, instead it wraps one of the existing wide-spread smart
         * pointer implementations and provides unified interface for them.
         */
        template<typename P>
        class AnyReferenceSmartPointer : public AnyReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AnyReferenceSmartPointer()
            {
                // No-op.
            }

            /**
             * Default constructor.
             */
            AnyReferenceSmartPointer() :
                ptr()
            {
                // No-op.
            }

            const void* Get() const
            {
                return reinterpret_cast<const void*>(&(*ptr));
            }

            void* Get()
            {
                return reinterpret_cast<void*>(&(*ptr));
            }

            /**
             * Swap contents with another instance.
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
         * Any reference implementation for the raw pointer.
         */
        template<typename T>
        class AnyReferenceOwningRawPointer : public AnyReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AnyReferenceOwningRawPointer()
            {
                delete ptr;
            }

            /**
             * Default constructor.
             */
            AnyReferenceOwningRawPointer() :
                ptr(0)
            {
                // No-op.
            }

            /**
             * Pointer constructor.
             *
             * @param ptr Pointer to take ownership over.
             */
            AnyReferenceOwningRawPointer(T* ptr) :
                ptr(ptr)
            {
                // No-op.
            }

            const void* Get() const
            {
                return reinterpret_cast<const void*>(ptr);
            }

            void* Get()
            {
                return reinterpret_cast<void*>(ptr);
            }

        private:
            /** Underlying pointer. */
            T* ptr;
        };

        /**
         * Any reference implementation for the raw pointer.
         */
        template<typename T>
        class AnyReferenceNonOwningRawPointer : public AnyReferenceImplBase
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AnyReferenceNonOwningRawPointer()
            {
                // No-op.
            }

            /**
             * Default constructor.
             */
            AnyReferenceNonOwningRawPointer() :
                ptr(0)
            {
                // No-op.
            }

            /**
             * Pointer constructor.
             *
             * @param ptr Pointer.
             */
            AnyReferenceNonOwningRawPointer(T* ptr) :
                ptr(ptr)
            {
                // No-op.
            }

            const void* Get() const
            {
                return reinterpret_cast<const void*>(ptr);
            }

            void* Get()
            {
                return reinterpret_cast<void*>(ptr);
            }

        private:
            /** Underlying pointer. */
            T* ptr;
        };

    }

    /**
     * Smart pointer class.
     *
     * It is special internal class which is used to wrap one of the existing
     * wide-spread smart pointer implementations.
     */
    template<typename T>
    class AnyReference
    {
        template<typename>
        friend class AnyReference;
    public:
        /**
         * Default constructor.
         */
        AnyReference() :
            ptr(),
            offset(0)
        {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param ptr Smart pointer implementation.
         * @param offset Pointer offset.
         */
        explicit AnyReference(common::AnyReferenceImplBase* ptr, ptrdiff_t offset = 0) :
            ptr(ptr),
            offset(offset)
        {
            // No-op.
        }

        /**
         * Copy constructor.
         *
         * @param other Another instance.
         */
        AnyReference(const AnyReference& other) :
            ptr(other.ptr),
            offset(other.offset)
        {
            // No-op.
        }

        /**
         * Copy constructor.
         *
         * @param other Another instance.
         */
        template<typename T2>
        AnyReference(const AnyReference<T2>& other) :
            ptr(other.ptr),
            offset(other.offset)
        {
            T2* p0 = reinterpret_cast<T2*>(80000);
            T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            offset += diff;
        }

        /**
         * Assignment operator.
         */
        AnyReference& operator=(const AnyReference& other)
        {
            ptr = other.ptr;
            offset = other.offset;

            return *this;
        }
        
        /**
         * Assignment operator.
         */
        template<typename T2>
        AnyReference& operator=(const AnyReference<T2>& other)
        {
            ptr = other.ptr;
            offset = other.offset;

            T2* p0 = reinterpret_cast<T2*>(80000);
            T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            offset += diff;

            return *this;
        }

        /**
         * Destructor.
         */
        ~AnyReference()
        {
            // No-op.
        }

        /**
         * Dereference the pointer.
         *
         * If the pointer is null then this operation causes undefined
         * behaviour.
         *
         * @return Constant reference to underlying value.
         */
        const T& Get() const
        {
            return *reinterpret_cast<const T*>(reinterpret_cast<ptrdiff_t>(ptr.Get()->Get()) + offset);
        }

        /**
         * Dereference the pointer.
         *
         * If the pointer is null then this operation causes undefined
         * behaviour.
         *
         * @return Reference to underlying value.
         */
        T& Get()
        {
            return *reinterpret_cast<T*>(reinterpret_cast<ptrdiff_t>(ptr.Get()->Get()) + offset);
        }

        /**
         * Check if the pointer is null.
         *
         * @return True if the value is null.
         */
        bool IsNull() const
        {
            const common::AnyReferenceImplBase* raw = ptr.Get();

            return !raw || !raw->Get();
        }

    private:
        /** Implementation. */
        common::concurrent::SharedPointer<common::AnyReferenceImplBase> ptr;

        /** Address offset. */
        ptrdiff_t offset;
    };

    /**
     * Used by user to pass smart pointers to Ignite API.
     *
     * @param ptr Pointer.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    AnyReference<typename T::element_type> PassSmartPointer(T ptr)
    {
        common::AnyReferenceSmartPointer<T>* impl = new common::AnyReferenceSmartPointer<T>();

        AnyReference<typename T::element_type> res(impl);

        impl->Swap(ptr);

        return res;
    }

    /**
     * Used to copy instance and store it as a AnyReference.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    AnyReference<T> PassCopy(const T& val)
    {
        common::AnyReferenceOwningRawPointer<T>* impl = new common::AnyReferenceOwningRawPointer<T>(new T(val));

        AnyReference<T> res(impl);

        return res;
    }

    /**
     * Used to pass reference and store it as a AnyReference.
     *
     * @param val Reference.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    AnyReference<T> PassReference(T& val)
    {
        common::AnyReferenceNonOwningRawPointer<T>* impl = new common::AnyReferenceNonOwningRawPointer<T>(&val);

        AnyReference<T> res(impl);

        return res;
    }
}

#endif //_IGNITE_COMMON_SMART_POINTER