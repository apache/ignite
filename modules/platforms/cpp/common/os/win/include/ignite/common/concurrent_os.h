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

#include <stdint.h>

#include <cassert>
#include <map>

#include <windows.h>

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
            class IGNITE_IMPORT_EXPORT Memory
            {
            public:
                /**
                 * Full fence.
                 */
                static void Fence();
            };

            /**
             * Critical section.
             */
            class IGNITE_IMPORT_EXPORT CriticalSection
            {
                friend class ConditionVariable;
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
                /** Handle. */
                CRITICAL_SECTION hnd;

                IGNITE_NO_COPY_ASSIGNMENT(CriticalSection)
            };

            class IGNITE_IMPORT_EXPORT ReadWriteLock
            {
            public:
                /**
                 * Constructor.
                 */
                ReadWriteLock();

                /**
                 * Destructor.
                 */
                ~ReadWriteLock();

                /**
                 * Lock in exclusive mode.
                 */
                void LockExclusive();

                /**
                 * Release in exclusive mode.
                 */
                void ReleaseExclusive();

                /**
                 * Lock in shared mode.
                 */
                void LockShared();

                /**
                 * Release in shared mode.
                 */
                void ReleaseShared();

            private:
                /** Lock. */
                SRWLOCK lock;

                IGNITE_NO_COPY_ASSIGNMENT(ReadWriteLock)
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
                /** Handle. */
                HANDLE hnd;

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
                 * Allocate thread-local index. Invoked once on DLL process attach.
                 *
                 * @return True if allocation was successful.
                 */
                static bool OnProcessAttach();

                /**
                 * Release thread-local entry. Invoked on DLL thread detach.
                 */
                static void OnThreadDetach();

                /**
                 * Release thread-local index. Invoked once on DLL process detach.
                 */
                static void OnProcessDetach();

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
                    void* winVal = Get0();

                    if (winVal)
                    {
                        std::map<int32_t, ThreadLocalEntry*>* map =
                            static_cast<std::map<int32_t, ThreadLocalEntry*>*>(winVal);

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
                    void* winVal = Get0();

                    if (winVal)
                    {
                        std::map<int32_t, ThreadLocalEntry*>* map =
                            static_cast<std::map<int32_t, ThreadLocalEntry*>*>(winVal);

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

                /**
                 * Internal thread-local map clear routine.
                 *
                 * @param mapPtr Pointer to map.
                 */
                static void Clear0(void* mapPtr);
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
                 * Destructor.
                 */
                ~ThreadLocalInstance()
                {
                    Remove();
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

            /**
             * Cross-platform wrapper for Condition Variable synchronization
             * primitive concept.
             */
            class ConditionVariable
            {
            public:
                /**
                 * Constructor.
                 */
                ConditionVariable()
                {
                    InitializeConditionVariable(&cond);
                }

                /**
                 * Destructor.
                 */
                ~ConditionVariable()
                {
                    // No-op.
                }

                /**
                 * Wait for Condition Variable to be notified.
                 *
                 * @param cs Critical section in which to wait.
                 */
                void Wait(CriticalSection& cs)
                {
                    SleepConditionVariableCS(&cond, &cs.hnd, INFINITE);
                }

                /**
                 * Wait for Condition Variable to be notified for specified time.
                 *
                 * @param cs Critical section in which to wait.
                 * @param msTimeout Timeout in milliseconds.
                 * @return True if the object has been notified and false in case of timeout.
                 */
                bool WaitFor(CriticalSection& cs, int32_t msTimeout)
                {
                    BOOL notified = SleepConditionVariableCS(&cond, &cs.hnd, msTimeout);

                    return notified != FALSE;
                }

                /**
                 * Notify single thread waiting for the condition variable.
                 */
                void NotifyOne()
                {
                    WakeConditionVariable(&cond);
                }

                /**
                 * Notify all threads that are waiting on the variable.
                 */
                void NotifyAll()
                {
                    WakeAllConditionVariable(&cond);
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ConditionVariable);

                /** OS-specific type. */
                CONDITION_VARIABLE cond;
            };

            /**
             * Manually triggered event.
             * Once triggered it stays in passing state until manually reset.
             */
            class ManualEvent
            {
            public:
                /**
                 * Constructs manual event.
                 * Initial state is untriggered.
                 */
                ManualEvent()
                {
                    handle = CreateEvent(NULL, TRUE, FALSE, NULL);

                    assert(handle != NULL);
                }

                /**
                 * Destructor.
                 */
                ~ManualEvent()
                {
                    CloseHandle(handle);
                }

                /**
                 * Sets event into triggered state.
                 */
                void Set()
                {
                    BOOL success = SetEvent(handle);

                    assert(success);
                }

                /**
                 * Resets event into non-triggered state.
                 */
                void Reset()
                {
                    BOOL success = ResetEvent(handle);

                    assert(success);
                }

                /**
                 * Wait for event to be triggered.
                 */
                void Wait()
                {
                    DWORD res = WaitForSingleObject(handle, INFINITE);

                    assert(res == WAIT_OBJECT_0);
                }

                /**
                 * Wait for event to be triggered for specified time.
                 *
                 * @param msTimeout Timeout in milliseconds.
                 * @return True if the object has been triggered and false in case of timeout.
                 */
                bool WaitFor(int32_t msTimeout)
                {
                    DWORD res = WaitForSingleObject(handle, static_cast<DWORD>(msTimeout));

                    assert(res == WAIT_OBJECT_0 || res == WAIT_TIMEOUT);

                    return res == WAIT_OBJECT_0;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ManualEvent);

                /** Event handle. */
                HANDLE handle;
            };
        }
    }
}

#endif //_IGNITE_COMMON_CONCURRENT_OS
