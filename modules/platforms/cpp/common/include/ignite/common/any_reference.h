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

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace common
    {
        /**
         * Smart pointer interface.
         */
        template<typename T>
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
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Constant reference to underlying value.
             */
            virtual const T& Get() const = 0;

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Reference to underlying value.
             */
            virtual T& Get() = 0;

            /**
             * Check if the pointer is null.
             *
             * @return True if the value is null.
             */
            virtual bool IsNull() const = 0;
        };

        /**
         * Smart pointer implementation class.
         *
         * Note, this class does not implement any smart pointer functionality
         * itself, instead it wraps one of the existing wide-spread smart
         * pointer implementations and provides unified interface for them.
         */
        template<typename P>
        class AnyReferenceSmartPointer : public AnyReferenceImplBase<typename P::element_type>
        {
        public:
            typedef typename P::element_type ElementType;

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

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Constant reference to underlying value.
             */
            const ElementType& Get() const
            {
                return *ptr;
            }

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Reference to underlying value.
             */
            ElementType& Get()
            {
                return *ptr;
            }

            /**
             * Check if the pointer is null.
             *
             * @return True if the value is null.
             */
            bool IsNull() const
            {
                return 0 == ptr.get();
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
        class AnyReferenceOwningRawPointer : public AnyReferenceImplBase<T>
        {
        public:
            typedef T ElementType;

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

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Constant reference to underlying value.
             */
            const ElementType& Get() const
            {
                return *ptr;
            }

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Reference to underlying value.
             */
            ElementType& Get()
            {
                return *ptr;
            }

            /**
             * Check if the pointer is null.
             *
             * @return True if the value is null.
             */
            bool IsNull() const
            {
                return 0 == ptr;
            }

        private:
            /** Underlying pointer. */
            T* ptr;
        };

        /**
         * Any reference implementation for the raw pointer.
         */
        template<typename T>
        class AnyReferenceNonOwningRawPointer : public AnyReferenceImplBase<T>
        {
        public:
            typedef T ElementType;

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

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Constant reference to underlying value.
             */
            const ElementType& Get() const
            {
                return *ptr;
            }

            /**
             * Dereference the pointer.
             *
             * If the pointer is null then this operation results in undefined
             * behaviour.
             *
             * @return Reference to underlying value.
             */
            ElementType& Get()
            {
                return *ptr;
            }

            /**
             * Check if the pointer is null.
             *
             * @return True if the value is null.
             */
            bool IsNull() const
            {
                return 0 == ptr;
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
    public:
        /**
         * Default constructor.
         */
        AnyReference() :
            ptr()
        {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param ptr Smart pointer implementation.
         */
        explicit AnyReference(common::AnyReferenceImplBase<T>* ptr) :
            ptr(ptr)
        {
            // No-op.
        }

        /**
         * Copy constructor.
         *
         * @param other Another instance.
         */
        AnyReference(const AnyReference& other) :
            ptr(other.ptr)
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
            ptr(other.GetPointer())
        {
            // No-op.
        }

        /**
         * Assignment operator.
         */
        AnyReference& operator=(const AnyReference& other)
        {
            ptr = other.ptr;

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
            return ptr.Get()->Get();
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
            return ptr.Get()->Get();
        }

        /**
         * Check if the pointer is null.
         *
         * @return True if the value is null.
         */
        bool IsNull() const
        {
            common::AnyReferenceImplBase<T>* raw = ptr.Get();

            return !raw || raw->IsNull();
        }

        /**
         * Get internal pointer.
         *
         * @return Internal pointer.
         */
        const common::concurrent::SharedPointer<common::AnyReferenceImplBase<T>>& GetPointer() const
        {
            return ptr;
        }

    private:
        /** Implementation. */
        common::concurrent::SharedPointer<common::AnyReferenceImplBase<T>> ptr;
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