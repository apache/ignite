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
    /**
     * Reference class.
     *
     * Abstraction on any reference-type object, from simple raw pointers and
     * references to standard library smart pointers.
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
         * @param other Another instance.
         */
        template<typename T2>
        Reference(const Reference<T2>& other) :
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
         * @param other Another instance.
         */
        template<typename T2>
        Reference& operator=(const Reference<T2>& other)
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
        ~Reference()
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
     * Used to pass smart pointers to Ignite API.
     *
     * @param ptr Pointer.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<typename T::element_type> PassSmartPointer(T ptr)
    {
        common::ReferenceSmartPointer<T>* impl = new common::ReferenceSmartPointer<T>();

        Reference<typename T::element_type> res(impl);

        impl->Swap(ptr);

        return res;
    }

    /**
     * Used to pass object copy to Ignite API.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> PassCopy(const T& val)
    {
        common::ReferenceOwningRawPointer<T>* impl = new common::ReferenceOwningRawPointer<T>(new T(val));

        return Reference<T>(impl);
    }

    /**
     * Used to pass object pointer to Ignite API.
     * Passed object deleted by Ignite when no longer needed.
     *
     * @param val Instance.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> PassOwnership(T* val)
    {
        common::ReferenceOwningRawPointer<T>* impl = new common::ReferenceOwningRawPointer<T>(val);

        return Reference<T>(impl);
    }

    /**
     * Used to pass object reference to Ignite API.
     * Ignite do not manage passed object and does not affect its lifetime.
     *
     * @param val Reference.
     * @return Implementation defined value. User should not explicitly use the
     *     returned value.
     */
    template<typename T>
    Reference<T> PassReference(T& val)
    {
        common::ReferenceNonOwningRawPointer<T>* impl = new common::ReferenceNonOwningRawPointer<T>(&val);

        return Reference<T>(impl);
    }
}

#endif //_IGNITE_COMMON_REFERENCE