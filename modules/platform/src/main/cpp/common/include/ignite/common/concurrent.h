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
                /**
                 * Constructor.
                 *
                 * @param ptr Raw pointer.
                 */
                SharedPointerImpl(void* ptr);

                /**
                 * Get raw pointer.
                 *
                 * @return Raw pointer.
                 */
                void* Pointer();

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

                /** Reference count. */
                int32_t refCnt;

                IGNITE_NO_COPY_ASSIGNMENT(SharedPointerImpl)
            };

            /**
             * Shared pointer.
             */
            template<typename T>
            class IGNITE_IMPORT_EXPORT SharedPointer
            {
            public:
                /**
                 * Constructor.
                 */
                SharedPointer() : impl(NULL), deleter(NULL)
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
                        impl = new SharedPointerImpl(ptr);
                        deleter = SharedPointerDefaultDeleter;
                    }
                    else
                    {
                        impl = NULL;
                        deleter = NULL;
                    }
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
                        this->impl = new SharedPointerImpl(ptr);
                        this->deleter = deleter;
                    }
                    else
                    {
                        this->impl = NULL;
                        this->deleter = NULL;
                    }
                }

                /**
                 * Copy constructor.
                 *
                 * @param other Instance to copy.
                 */
                SharedPointer(const SharedPointer& other)
                {
                    impl = other.impl;
                    deleter = other.deleter;

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
                        // 1. Create new instance.
                        SharedPointer tmp(other);

                        // 2. Swap with temp.
                        SharedPointerImpl* impl0 = impl;
                        void(*deleter0)(T*) = deleter;

                        impl = tmp.impl;
                        deleter = tmp.deleter;

                        tmp.impl = impl0;
                        tmp.deleter = deleter0;
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

                        delete impl;

                        deleter(ptr);
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
            private:
                /** Implementation. */
                SharedPointerImpl* impl;

                /** Delete function. */
                void(*deleter)(T*);
            };
        }
    }
}

#endif