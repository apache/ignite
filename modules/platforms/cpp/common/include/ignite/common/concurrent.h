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

#ifndef _IGNITE_COMMON_CONCURRENT
#define _IGNITE_COMMON_CONCURRENT

#include <cassert>

#include "ignite/common/concurrent_os.h"

namespace ignite
{
    namespace common
    {
        namespace concurrent
        {
            /**
             * Default deleter implementation.
             *
             * @param obj Object to be deleted.
             */
            template<typename T>
            IGNITE_IMPORT_EXPORT void SharedPointerDefaultDeleter(T* obj)
            {
                delete obj;
            }

            /**
             * Holder of shared pointer data.
             */
            class IGNITE_IMPORT_EXPORT SharedPointerImpl
            {
            public:
                typedef void(*DeleterType)(void*);
                /**
                 * Constructor.
                 *
                 * @param ptr Raw pointer.
                 */
                SharedPointerImpl(void* ptr, DeleterType deleter);

                /**
                 * Get raw pointer.
                 *
                 * @return Raw pointer.
                 */
                void* Pointer();

                /**
                 * Get raw pointer.
                 *
                 * @return Raw pointer.
                 */
                const void* Pointer() const;

                /**
                 * Get raw pointer.
                 *
                 * @return Raw pointer.
                 */
                DeleterType Deleter();

                /**
                 * Increment usage counter.
                 */
                void Increment();

                /**
                 * Decrement usage counter.
                 *
                 * @return True if counter reached zero.
                 */
                bool Decrement();
            private:
                /** Raw pointer. */
                void* ptr;

                /** Deleter. */
                DeleterType deleter;

                /** Reference count. */
                int32_t refCnt;

                IGNITE_NO_COPY_ASSIGNMENT(SharedPointerImpl)
            };

            /* Forward declaration. */
            template<typename T>
            class IGNITE_IMPORT_EXPORT EnableSharedFromThis;

            /* Forward declaration. */
            template<typename T>
            inline void ImplEnableShared(EnableSharedFromThis<T>* some, SharedPointerImpl* impl);

            // Do nothing if the instance is not derived from EnableSharedFromThis.
            inline void ImplEnableShared(const volatile void*, const volatile void*)
            {
                // No-op.
            }

            /**
             * Shared pointer.
             */
            template<typename T>
            class IGNITE_IMPORT_EXPORT SharedPointer
            {
            public:
                friend class EnableSharedFromThis<T>;

                /**
                 * Constructor.
                 */
                SharedPointer() : impl(0)
                {
                    // No-op.
                }

                /**
                 * Constructor.
                 *
                 * @param ptr Raw pointer.
                 */
                explicit SharedPointer(T* ptr)
                {
                    if (ptr)
                    {
                        impl = new SharedPointerImpl(ptr, reinterpret_cast<SharedPointerImpl::DeleterType>(&SharedPointerDefaultDeleter<T>));
                        ImplEnableShared(ptr, impl);
                    }
                    else
                        impl = 0;
                }

                /**
                 * Constructor.
                 *
                 * @param ptr Raw pointer.
                 * @param deleter Delete function.
                 */
                SharedPointer(T* ptr, void(*deleter)(T*))
                {
                    if (ptr)
                    {
                        impl = new SharedPointerImpl(ptr, reinterpret_cast<SharedPointerImpl::DeleterType>(deleter));
                        ImplEnableShared(ptr, impl);
                    }
                    else
                        impl = 0;
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Instance to copy.
                 */
                SharedPointer(const SharedPointer& other)
                {
                    impl = other.impl;

                    if (impl)
                        impl->Increment();
                }

                /**
                 * Assignment operator.
                 *
                 * @param other Other instance.
                 */
                SharedPointer& operator=(const SharedPointer& other)
                {
                    if (this != &other)
                    {
                        SharedPointer tmp(other);

                        std::swap(impl, tmp.impl);
                    }

                    return *this;
                }

                /**
                 * Destructor.
                 */
                ~SharedPointer()
                {
                    if (impl && impl->Decrement())
                    {
                        T* ptr = Get();

                        void(*deleter)(T*) = reinterpret_cast<void(*)(T*)>(impl->Deleter());

                        deleter(ptr);

                        delete impl;
                    }
                }

                /**
                 * Get raw pointer.
                 *
                 * @return Raw pointer.
                 */
                T* Get()
                {
                    return impl ? static_cast<T*>(impl->Pointer()) : NULL;
                }

                /**
                 * Get raw pointer.
                 *
                 * @return Raw pointer.
                 */
                const T* Get() const
                {
                    return impl ? static_cast<T*>(impl->Pointer()) : NULL;
                }

                /**
                 * Check whether underlying raw pointer is valid.
                 *
                 * @return True if valid.
                 */
                bool IsValid() const
                {
                    return impl != NULL;
                }
            private:
                /** Implementation. */
                SharedPointerImpl* impl;
            };

            /**
             * The class provides functionality that allows objects of derived
             * classes to create instances of shared_ptr pointing to themselves
             * and sharing ownership with existing shared_ptr objects.
             */
            template<typename T>
            class IGNITE_IMPORT_EXPORT EnableSharedFromThis
            {
            public:
                /**
                 * Default constructor.
                 */
                EnableSharedFromThis() : self(0)
                {
                    // No-op.
                }

                /**
                 * Copy constructor.
                 */
                EnableSharedFromThis(const EnableSharedFromThis&) : self(0)
                {
                    // No-op.
                }

                /**
                 * Assignment operator.
                 */
                EnableSharedFromThis& operator=(const EnableSharedFromThis&)
                {
                    return *this;
                }

                /**
                 * Destructor.
                 */
                virtual ~EnableSharedFromThis()
                {
                    // No-op.
                }

                /**
                 * Create shared pointer for this instance.
                 *
                 * Can only be called on already shared object.
                 * @return New shared pointer instance.
                 */
                SharedPointer<T> SharedFromThis()
                {
                    assert(self != 0);

                    SharedPointer<T> ptr;

                    ptr.impl = self;

                    self->Increment();

                    return ptr;
                }

            private:
                template<typename T0>
                friend void ImplEnableShared(EnableSharedFromThis<T0>*, SharedPointerImpl*);

                /** Shared pointer base. */
                SharedPointerImpl* self;
            };

            // Implementation for instances derived from EnableSharedFromThis.
            template<typename T>
            inline void ImplEnableShared(EnableSharedFromThis<T>* some, SharedPointerImpl* impl)
            {
                if (some)
                    some->self = impl;
            }

            /**
             * Lock guard.
             */
            template<typename T>
            class LockGuard
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param lock Lockable object.
                 */
                LockGuard(T& lock) :
                    lock(lock)
                {
                    lock.Enter();
                }

                /**
                 * Destructor.
                 */
                ~LockGuard()
                {
                    lock.Leave();
                }

            private:
                T& lock;
            };

            typedef LockGuard<CriticalSection> CsLockGuard;
        }
    }
}

#endif