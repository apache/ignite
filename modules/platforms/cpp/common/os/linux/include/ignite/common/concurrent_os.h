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

#ifndef _IGNITE_COMMON_CONCURRENT_OS
#define _IGNITE_COMMON_CONCURRENT_OS

#include <map>
#include <stdint.h>
#include <pthread.h>

#include "ignite/common/common.h"

namespace ignite
{
    namespace common
    {
        namespace concurrent
        {
            /**
             * Static class to manage memory visibility semantics. 
             */
            class IGNITE_IMPORT_EXPORT Memory {
            public:
                /**
                 * Full fence. 
                 */
                static void Fence();
            };

            /**
             * Critical section.
             */
            class IGNITE_IMPORT_EXPORT CriticalSection {
            public:
                /**
                 * Constructor.
                 */
                CriticalSection();

                /**
                 * Destructor. 
                 */
                ~CriticalSection();

                /**
                 * Enter critical section.
                 */
                void Enter();

                /**
                 * Leave critical section.
                 */
                void Leave();
            private:
                pthread_mutex_t mux;
                
                IGNITE_NO_COPY_ASSIGNMENT(CriticalSection)
            };

            /**
             * Special latch with count = 1.
             */
            class IGNITE_IMPORT_EXPORT SingleLatch
            {                
            public:
                /**
                 * Constructor.
                 */
                SingleLatch();

                /**
                 * Destructor.
                 */
                ~SingleLatch();

                /**
                 * Perform the countdown.
                 */
                void CountDown();

                /**
                 * Await the countdown.
                 */
                void Await();
            private:
                /** Mutex. */
                pthread_mutex_t mux;

                /** Condition. */
                pthread_cond_t cond;

                /** Ready flag. */
                bool ready;
                
                IGNITE_NO_COPY_ASSIGNMENT(SingleLatch)
            };

            /**
             * Primitives for atomic access.
             */
            class IGNITE_IMPORT_EXPORT Atomics
            {
            public:
                /**
                 * Update the 32-bit integer value if it is equal to expected value.
                 *
                 * @param ptr Pointer.
                 * @param expVal Expected value.
                 * @param newVal New value.
                 * @return True if update occurred as a result of this call, false otherwise.
                 */
                static bool CompareAndSet32(int32_t* ptr, int32_t expVal, int32_t newVal);

                /**
                 * Update the 32-bit integer value if it is equal to expected value.
                 *
                 * @param ptr Pointer.
                 * @param expVal Expected value.
                 * @param newVal New value.
                 * @return Value which were observed during CAS attempt.
                 */
                static int32_t CompareAndSet32Val(int32_t* ptr, int32_t expVal, int32_t newVal);

                /**
                 * Increment 32-bit integer and return new value.
                 *
                 * @param ptr Pointer.
                 * @return Value after increment.
                 */
                static int32_t IncrementAndGet32(int32_t* ptr);

                /**
                 * Decrement 32-bit integer and return new value.
                 *
                 * @param ptr Pointer.
                 * @return Value after decrement.
                 */
                static int32_t DecrementAndGet32(int32_t* ptr);

                /**
                 * Update the 64-bit integer value if it is equal to expected value.
                 *
                 * @param ptr Pointer.
                 * @param expVal Expected value.
                 * @param newVal New value.
                 * @return True if update occurred as a result of this call, false otherwise.
                 */
                static bool CompareAndSet64(int64_t* ptr, int64_t expVal, int64_t newVal);

                /**
                 * Update the 64-bit integer value if it is equal to expected value.
                 *
                 * @param ptr Pointer.
                 * @param expVal Expected value.
                 * @param newVal New value.
                 * @return Value which were observed during CAS attempt.
                 */
                static int64_t CompareAndSet64Val(int64_t* ptr, int64_t expVal, int64_t newVal);

                /**
                 * Increment 64-bit integer and return new value.
                 *
                 * @param ptr Pointer.
                 * @return Value after increment.
                 */
                static int64_t IncrementAndGet64(int64_t* ptr);

                /**
                 * Decrement 64-bit integer and return new value.
                 *
                 * @param ptr Pointer.
                 * @return Value after decrement.
                 */
                static int64_t DecrementAndGet64(int64_t* ptr);
            };

            /**
             * Thread-local entry.
             */
            class IGNITE_IMPORT_EXPORT ThreadLocalEntry
            {
            public:
                /**
                 * Virtual destructor to allow for correct typed entries cleanup.
                 */
                virtual ~ThreadLocalEntry()
                {
                    // No-op.
                }
            };

            /**
             * Typed thread-local entry.
             */
            template<typename T>
            class IGNITE_IMPORT_EXPORT ThreadLocalTypedEntry : public ThreadLocalEntry
            {
            public:
                /**
                 * Constructor.
                 *
                 * @param val Value.
                 */
                ThreadLocalTypedEntry(T val) : val(val)
                {
                    // No-op.
                }

                ~ThreadLocalTypedEntry()
                {
                    // No-op.
                }

                /**
                 * Get value.
                 *
                 * @return Value.
                 */
                T Get()
                {
                    return val;
                }
            private:
                /** Value. */
                T val;
            };

            /**
             * Thread-local abstraction.
             */
            class IGNITE_IMPORT_EXPORT ThreadLocal
            {
            public:
                /**
                 * Get next available index to be used in thread-local storage.
                 *
                 * @return Index.
                 */
                static int32_t NextIndex();

                /**
                 * Get value by index.
                 *
                 * @param idx Index.
                 * @return Value associated with the index or NULL.
                 */
                template<typename T>
                static T Get(int32_t idx)
                {
                    void* linuxVal = Get0();

                    if (linuxVal)
                    {
                        std::map<int32_t, ThreadLocalEntry*>* map =
                            static_cast<std::map<int32_t, ThreadLocalEntry*>*>(linuxVal);

                        ThreadLocalTypedEntry<T>* entry = static_cast<ThreadLocalTypedEntry<T>*>((*map)[idx]);

                        if (entry)
                            return entry->Get();
                    }

                    return T();
                }

                /**
                 * Set value at the given index.
                 *
                 * @param idx Index.
                 * @param val Value to be associated with the index.
                 */
                template<typename T>
                static void Set(int32_t idx, const T& val)
                {
                    void* linuxVal = Get0();

                    if (linuxVal)
                    {
                        std::map<int32_t, ThreadLocalEntry*>* map =
                            static_cast<std::map<int32_t, ThreadLocalEntry*>*>(linuxVal);

                        ThreadLocalEntry* appVal = (*map)[idx];

                        if (appVal)
                            delete appVal;

                        (*map)[idx] = new ThreadLocalTypedEntry<T>(val);
                    }
                    else
                    {
                        std::map<int32_t, ThreadLocalEntry*>* map = new std::map<int32_t, ThreadLocalEntry*>();

                        Set0(map);

                        (*map)[idx] = new ThreadLocalTypedEntry<T>(val);
                    }
                }

                /**
                 * Remove value at the given index.
                 *
                 * @param idx Index.
                 */
                static void Remove(int32_t idx);

                /**
                 * Internal thread-local map clear routine.
                 *
                 * @param mapPtr Pointer to map.
                 */
                static void Clear0(void* mapPtr);

            private:
                /**
                 * Internal get routine.
                 *
                 * @param Associated value.
                 */
                static void* Get0();

                /**
                 * Internal set routine.
                 *
                 * @param ptr Pointer.
                 */
                static void Set0(void* ptr);
            };

            /**
             * Thread-local instance. Simplifies API avoiding direct index allocations.
             */
            template<typename T>
            class IGNITE_IMPORT_EXPORT ThreadLocalInstance
            {
            public:
                /**
                 * Constructor.
                 */
                ThreadLocalInstance() : idx(ThreadLocal::NextIndex())
                {
                    // No-op.
                }

                /**
                 * Get value.
                 *
                 * @return Value.
                 */
                T Get()
                {
                    return ThreadLocal::Get<T>(idx);
                }

                /**
                 * Set instance.
                 *
                 * @param val Value.
                 */
                void Set(const T& val)
                {
                    ThreadLocal::Set<T>(idx, val);
                }

                /**
                 * Remove instance.
                 */
                void Remove()
                {
                    ThreadLocal::Remove(idx);
                }

            private:
                /** Index. */
                int32_t idx;
            };
        }
    }
}

#endif