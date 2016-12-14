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
 * Declares ignite::Reference class.
 */

#ifndef _IGNITE_COMMON_REFERENCE
#define _IGNITE_COMMON_REFERENCE

#include <cstddef>

#include <ignite/common/common.h>
#include <ignite/common/concurrent.h>
#include <ignite/common/reference_impl.h>

namespace ignite
{
    template<typename T>
    class Reference;

    /**
     * Constant Reference class.
     *
     * Abstraction on any reference-type object, from simple raw pointers and
     * references to standard library smart pointers. Provides only constant
     * access to the underlying data.
     *
     * There are no requirements for the template type T.
     */
    template<typename T>
    class ConstReference
    {
        template<typename>
        friend class ConstReference;

        template<typename>
        friend class Reference;

    public:
        /**
         * Default constructor.
         */
        ConstReference() :
            ptr(),
            offset(0)
        {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param ptr ConstReference class implementation.
         * @param offset Pointer offset.
         */
        explicit ConstReference(common::ConstReferenceImplBase* ptr, ptrdiff_t offset = 0) :
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
        ConstReference(const ConstReference& other) :
            ptr(other.ptr),
            offset(other.offset)
        {
            // No-op.
        }

        /**
         * Copy constructor.
         *
         * Constant reference of type T2 should be static-castable to constant
         * reference of type T.
         *
         * @param other Another instance.
         */
        template<typename T2>
        ConstReference(const ConstReference<T2>& other) :
            ptr(other.ptr),
            offset(other.offset)
        {
            T2* p0 = reinterpret_cast<T2*>(common::POINTER_CAST_MAGIC_NUMBER);
            T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            offset += diff;
        }

        /**
         * Assignment operator.
         *
         * @param other Another instance.
         */
        ConstReference& operator=(const ConstReference& other)
        {
            ptr = other.ptr;
            offset = other.offset;

            return *this;
        }
        
        /**
         * Assignment operator.
         *
         * Constant reference of type T2 should be static-castable to constant
         * reference of type T.
         *
         * @param other Another instance.
         */
        template<typename T2>
        ConstReference& operator=(const ConstReference<T2>& other)
        {
            ptr = other.ptr;
            offset = other.offset;

            T2* p0 = reinterpret_cast<T2*>(common::POINTER_CAST_MAGIC_NUMBER);
            T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            offset += diff;

            return *this;
        }

        /**
         * Destructor.
         */
        ~ConstReference()
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
         * Check if the pointer is null.
         *
         * @return True if the value is null.
         */
        bool IsNull() const
        {
            const common::ConstReferenceImplBase* raw = ptr.Get();

            return !raw || !raw->Get();
        }

    private:
        /** Implementation. */
        common::concurrent::SharedPointer<common::ConstReferenceImplBase> ptr;

        /** Address offset. */
        ptrdiff_t offset;
    };

    /**
     * Reference class.
     *
     * Abstraction on any reference-type object, from simple raw pointers and
     * references to standard library smart pointers.
     *
     * There are no requirements for the template type T.
     */
    template<typename T>
    class Reference
    {
        template<typename>
        friend class Reference;
    public:
        /**
         * Default constructor.
         */
        Reference() :
            ptr(),
            offset(0)
        {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param ptr Reference class implementation.
         * @param offset Pointer offset.
         */
        explicit Reference(common::ReferenceImplBase* ptr, ptrdiff_t offset = 0) :
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
        Reference(const Reference& other) :
            ptr(other.ptr),
            offset(other.offset)
        {
            // No-op.
        }

        /**
         * Copy constructor.
         * 
         * Reference of type T2 should be static-castable to reference of type T.
         *
         * @param other Another instance.
         */
        template<typename T2>
        Reference(const Reference<T2>& other) :
            ptr(other.ptr),
            offset(other.offset)
        {
            T2* p0 = reinterpret_cast<T2*>(common::POINTER_CAST_MAGIC_NUMBER);
            T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            offset += diff;
        }

        /**
         * Assignment operator.
         *
         * @param other Another instance.
         */
        Reference& operator=(const Reference& other)
        {
            ptr = other.ptr;
            offset = other.offset;

            return *this;
        }
        
        /**
         * Assignment operator.
         *
         * Reference of type T2 should be static-castable to reference of type T.
         *
         * @param other Another instance.
         */
        template<typename T2>
        Reference& operator=(const Reference<T2>& other)
        {
            ptr = other.ptr;
            offset = other.offset;

            T2* p0 = reinterpret_cast<T2*>(common::POINTER_CAST_MAGIC_NUMBER);
            T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            offset += diff;

            return *this;
        }

        /**
         * Destructor.
         */
        ~Reference()
        {
            // No-op.
        }

        /**
         * Const cast operator.
         *
         * Reference of type T2 should be static-castable to reference of type T.
         *
         * Casts this instance to constant reference.
         */
        template<typename T2>
        operator ConstReference<T2>()
        {
            ConstReference<T2> cr;

            cr.ptr = ptr;
            cr.offset = offset;

            T2* p0 = reinterpret_cast<T2*>(common::POINTER_CAST_MAGIC_NUMBER);
            const T* p1 = static_cast<T*>(p0);

            ptrdiff_t diff = reinterpret_cast<ptrdiff_t>(p1) - reinterpret_cast<ptrdiff_t>(p0);
            cr.offset -= diff;

            return cr;
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
            const common::ReferenceImplBase* raw = ptr.Get();

            return !raw || !raw->Get();
        }

    private:
        /** Implementation. */
        common::concurrent::SharedPointer<common::ReferenceImplBase> ptr;

        /** Address offset. */
        ptrdiff_t offset;
    };

    /**
     * Make ignite::Reference instance out of smart pointer.
     *
     * Template type 'T' should be a smart pointer and provide pointer semantics:
     * - There should be defined type 'T::element_type', showing underlying type.
     * - Type 'T' should be dereferencible (should have operators
     *   T::element_type& operator*() and const T::element_type& operator*() const).
     * - Operation std::swap should result in valid result if applied to two
     *   instances of that type.
     *
     * @param ptr Pointer.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<typename T::element_type> MakeReferenceFromSmartPointer(T ptr)
    {
        common::ReferenceSmartPointer<T>* impl = new common::ReferenceSmartPointer<T>();

        Reference<typename T::element_type> res(impl);

        impl->Swap(ptr);

        return res;
    }

    /**
     * Make ignite::ConstReference instance out of smart pointer.
     *
     * Template type 'T' should be a smart pointer and provide pointer semantics:
     * - There should be defined type 'T::element_type', showing underlying type.
     * - Type 'T' should be dereferencible (should have operators
     *   T::element_type& operator*() and const T::element_type& operator*() const).
     * - Operation std::swap should result in valid result if applied to two
     *   instances of that type.
     *
     * @param ptr Pointer.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    ConstReference<typename T::element_type> MakeConstReferenceFromSmartPointer(T ptr)
    {
        common::ReferenceSmartPointer<T>* impl = new common::ReferenceSmartPointer<T>();

        ConstReference<typename T::element_type> res(impl);

        impl->Swap(ptr);

        return res;
    }

    /**
     * Copy object and wrap it to make ignite::Reference instance.
     *
     * Template type 'T' should be copy-constructible.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> MakeReferenceFromCopy(const T& val)
    {
        common::ReferenceOwningRawPointer<T>* impl = new common::ReferenceOwningRawPointer<T>(new T(val));

        return Reference<T>(impl);
    }

    /**
     * Copy object and wrap it to make ignite::ConstReference instance.
     *
     * Template type 'T' should be copy-constructible.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    ConstReference<T> MakeConstReferenceFromCopy(const T& val)
    {
        common::ReferenceOwningRawPointer<T>* impl = new common::ReferenceOwningRawPointer<T>(new T(val));

        return ConstReference<T>(impl);
    }

    /**
     * Make ignite::Reference instance out of pointer and pass its ownership.
     * Passed object deleted by Ignite when no longer needed.
     *
     * There are no requirements for the template type T.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> MakeReferenceFromOwningPointer(T* val)
    {
        common::ReferenceOwningRawPointer<T>* impl = new common::ReferenceOwningRawPointer<T>(val);

        return Reference<T>(impl);
    }

    /**
     * Make ignite::ConstReference instance out of pointer and pass its ownership.
     * Passed object deleted by Ignite when no longer needed.
     *
     * There are no requirements for the template type T.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    ConstReference<T> MakeConstReferenceFromOwningPointer(T* val)
    {
        common::ReferenceOwningRawPointer<T>* impl = new common::ReferenceOwningRawPointer<T>(val);

        return ConstReference<T>(impl);
    }

    /**
     * Make ignite::Reference instance out of reference.
     * Ignite do not manage passed object and does not affect its lifetime.
     *
     * There are no requirements for the template type T.
     *
     * @param val Reference.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> MakeReference(T& val)
    {
        common::ReferenceNonOwningRawPointer<T>* impl = new common::ReferenceNonOwningRawPointer<T>(&val);

        return Reference<T>(impl);
    }

    /**
     * Make ignite::Reference instance out of pointer.
     * Ignite do not manage passed object and does not affect its lifetime.
     *
     * There are no requirements for the template type T.
     *
     * @param val Reference.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> MakeReference(T* val)
    {
        common::ReferenceNonOwningRawPointer<T>* impl = new common::ReferenceNonOwningRawPointer<T>(val);

        return Reference<T>(impl);
    }

    /**
     * Make ignite::ConstReference instance out of constant reference.
     * Ignite do not manage passed object and does not affect its lifetime.
     *
     * There are no requirements for the template type T.
     *
     * @param val Reference.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    ConstReference<T> MakeConstReference(const T& val)
    {
        common::ConstReferenceNonOwningRawPointer<T>* impl = new common::ConstReferenceNonOwningRawPointer<T>(&val);

        return ConstReference<T>(impl);
    }

    /**
     * Make ignite::ConstReference instance out of constant pointer.
     * Ignite do not manage passed object and does not affect its lifetime.
     *
     * There are no requirements for the template type T.
     *
     * @param val Reference.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    ConstReference<T> MakeConstReference(const T* val)
    {
        common::ConstReferenceNonOwningRawPointer<T>* impl = new common::ConstReferenceNonOwningRawPointer<T>(val);

        return ConstReference<T>(impl);
    }
}

#endif //_IGNITE_COMMON_REFERENCE