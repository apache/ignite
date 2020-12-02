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

#include <pthread.h>
#include <time.h>
#include <errno.h>

#include <stdint.h>

#include <cassert>
#include <map>

#include <ignite/common/common.h>

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
                pthread_mutex_t mux;
                
                IGNITE_NO_COPY_ASSIGNMENT(CriticalSection);
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
                pthread_rwlock_t lock;

                IGNITE_NO_COPY_ASSIGNMENT(ReadWriteLock);
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
                
                IGNITE_NO_COPY_ASSIGNMENT(SingleLatch);
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
                    pthread_condattr_t attr;
                    int err = pthread_condattr_init(&attr);
                    assert(!err);
                    IGNITE_UNUSED(err);

#if !defined(__APPLE__)
                    err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
                    assert(!err);
                    IGNITE_UNUSED(err);
#endif
                    err = pthread_cond_init(&cond, &attr);
                    assert(!err);
                    IGNITE_UNUSED(err);
                }

                /**
                 * Destructor.
                 */
                ~ConditionVariable()
                {
                    pthread_cond_destroy(&cond);
                }

                /**
                 * Wait for Condition Variable to be notified.
                 *
                 * @param cs Critical section in which to wait.
                 */
                void Wait(CriticalSection& cs)
                {
                    pthread_cond_wait(&cond, &cs.mux);
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
                    timespec ts;
                    int err = clock_gettime(CLOCK_MONOTONIC, &ts);
                    assert(!err);

                    IGNITE_UNUSED(err);

                    ts.tv_sec += msTimeout / 1000 + (ts.tv_nsec + (msTimeout % 1000) * 1000000) / 1000000000;
                    ts.tv_nsec = (ts.tv_nsec + (msTimeout % 1000) * 1000000) % 1000000000;

                    int res = pthread_cond_timedwait(&cond, &cs.mux, &ts);

                    return res == 0;
                }

                /**
                 * Notify single thread waiting for the condition variable.
                 */
                void NotifyOne()
                {
                    int err = pthread_cond_signal(&cond);

                    assert(!err);

                    IGNITE_UNUSED(err);
                }

                /**
                 * Notify all threads that are waiting on the variable.
                 */
                void NotifyAll()
                {
                    int err = pthread_cond_broadcast(&cond);

                    assert(!err);

                    IGNITE_UNUSED(err);
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ConditionVariable);

                /** OS-specific type. */
                pthread_cond_t cond;
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
                ManualEvent() :
                    cond(),
                    mutex(),
                    state(false)
                {
                    pthread_condattr_t attr;
                    int err = pthread_condattr_init(&attr);
                    assert(!err);
                    IGNITE_UNUSED(err);

#if !defined(__APPLE__)
                    err = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
                    assert(!err);
                    IGNITE_UNUSED(err);
#endif

                    err = pthread_cond_init(&cond, &attr);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    err = pthread_mutex_init(&mutex, NULL);
                    assert(!err);
                    IGNITE_UNUSED(err);
                }

                /**
                 * Destructor.
                 */
                ~ManualEvent()
                {
                    pthread_mutex_destroy(&mutex);
                    pthread_cond_destroy(&cond);
                }

                /**
                 * Sets event into triggered state.
                 */
                void Set()
                {
                    int err = pthread_mutex_lock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    state = true;

                    err = pthread_cond_broadcast(&cond);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    err = pthread_mutex_unlock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);
                }

                /**
                 * Resets event into non-triggered state.
                 */
                void Reset()
                {
                    int err = pthread_mutex_lock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    state = false;

                    err = pthread_mutex_unlock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);
                }

                /**
                 * Wait for event to be triggered.
                 */
                void Wait()
                {
                    int err = pthread_mutex_lock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    while (!state)
                    {
                        err = pthread_cond_wait(&cond, &mutex);
                        assert(!err);
                        IGNITE_UNUSED(err);
                    }

                    err = pthread_mutex_unlock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);
                }

                /**
                 * Wait for event to be triggered for specified time.
                 *
                 * @param msTimeout Timeout in milliseconds.
                 * @return True if the object has been triggered and false in case of timeout.
                 */
                bool WaitFor(int32_t msTimeout)
                {
                    int res = 0;
                    int err = pthread_mutex_lock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    if (!state)
                    {
                        timespec ts;
                        err = clock_gettime(CLOCK_MONOTONIC, &ts);
                        assert(!err);
                        IGNITE_UNUSED(err);

                        ts.tv_sec += msTimeout / 1000 + (ts.tv_nsec + (msTimeout % 1000) * 1000000) / 1000000000;
                        ts.tv_nsec = (ts.tv_nsec + (msTimeout % 1000) * 1000000) % 1000000000;

                        res = pthread_cond_timedwait(&cond, &mutex, &ts);
                        assert(res == 0 || res == ETIMEDOUT);
                        IGNITE_UNUSED(res);
                    }

                    err = pthread_mutex_unlock(&mutex);
                    assert(!err);
                    IGNITE_UNUSED(err);

                    return res == 0;
                }

            private:
                IGNITE_NO_COPY_ASSIGNMENT(ManualEvent);

                /** Condition variable. */
                pthread_cond_t cond;

                /** Mutex. */
                pthread_mutex_t mutex;

                /** State. */
                bool state;
            };
        }
    }
}

#endif
