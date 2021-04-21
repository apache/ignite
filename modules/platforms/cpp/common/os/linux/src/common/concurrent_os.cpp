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

#include "ignite/common/concurrent_os.h"

namespace ignite
{
    namespace common
    {
        namespace concurrent
        {
            /** Key indicating that the thread is attached. */
            static pthread_key_t tlsKey;

            /** Helper to ensure that attach key is allocated only once. */
            static pthread_once_t tlsKeyInit = PTHREAD_ONCE_INIT;

            /**
             * Routine to destroy TLS key.
             *
             * @param key Key.
             */
            void DestroyTlsKey(void* key) {
                ThreadLocal::Clear0(key);
            }

            /**
             * Routine to allocate TLS key.
             */
            void AllocateTlsKey() {
                pthread_key_create(&tlsKey, DestroyTlsKey);
            }

            void Memory::Fence() {
                __asm__ volatile ("" ::: "memory");
            }

            CriticalSection::CriticalSection() {
                pthread_mutex_init(&mux, NULL);

                Memory::Fence();
            }

            CriticalSection::~CriticalSection() {
                Memory::Fence();

                pthread_mutex_destroy(&mux);
            }

            void CriticalSection::Enter() {
                Memory::Fence();

                pthread_mutex_lock(&mux);
            }

            void CriticalSection::Leave() {
                Memory::Fence();

                pthread_mutex_unlock(&mux);
            }

            ReadWriteLock::ReadWriteLock() :
                lock()
            {
                pthread_rwlock_init(&lock, NULL);

                Memory::Fence();
            }

            ReadWriteLock::~ReadWriteLock()
            {
                pthread_rwlock_destroy(&lock);
            }

            void ReadWriteLock::LockExclusive()
            {
                pthread_rwlock_wrlock(&lock);
            }

            void ReadWriteLock::ReleaseExclusive()
            {
                pthread_rwlock_unlock(&lock);
            }

            void ReadWriteLock::LockShared()
            {
                pthread_rwlock_rdlock(&lock);
            }

            void ReadWriteLock::ReleaseShared()
            {
                pthread_rwlock_unlock(&lock);
            }

            SingleLatch::SingleLatch()
            {
                pthread_mutex_init(&mux, NULL);
                pthread_cond_init(&cond, NULL);
                ready = false;

                Memory::Fence();
            }

            SingleLatch::~SingleLatch()
            {
                Memory::Fence();

                pthread_cond_destroy(&cond);
                pthread_mutex_destroy(&mux);
            }

            void SingleLatch::CountDown()
            {
                pthread_mutex_lock(&mux);

                if (!ready) {
                    ready = true;

                    pthread_cond_broadcast(&cond);
                }

                pthread_mutex_unlock(&mux);

                Memory::Fence();
            }

            void SingleLatch::Await()
            {
                pthread_mutex_lock(&mux);

                while (!ready)
                    pthread_cond_wait(&cond, &mux);

                pthread_mutex_unlock(&mux);

                Memory::Fence();
            }

            bool Atomics::CompareAndSet32(int32_t* ptr, int32_t expVal, int32_t newVal)
            {
                return __sync_bool_compare_and_swap(ptr, expVal, newVal);
            }

            int32_t Atomics::CompareAndSet32Val(int32_t* ptr, int32_t expVal, int32_t newVal)
            {
                return __sync_val_compare_and_swap(ptr, expVal, newVal);
            }

            int32_t Atomics::IncrementAndGet32(int32_t* ptr)
            {
               return __sync_fetch_and_add(ptr, 1) + 1;
            }

            int32_t Atomics::DecrementAndGet32(int32_t* ptr)
            {
               return __sync_fetch_and_sub(ptr, 1) - 1;
            }

            bool Atomics::CompareAndSet64(int64_t* ptr, int64_t expVal, int64_t newVal)
            {
               return __sync_bool_compare_and_swap(ptr, expVal, newVal);
            }

            int64_t Atomics::CompareAndSet64Val(int64_t* ptr, int64_t expVal, int64_t newVal)
            {
               return __sync_val_compare_and_swap(ptr, expVal, newVal);
            }

            int64_t Atomics::IncrementAndGet64(int64_t* ptr)
            {
               return __sync_fetch_and_add(ptr, 1) + 1;
            }

            int64_t Atomics::DecrementAndGet64(int64_t* ptr)
            {
               return __sync_fetch_and_sub(ptr, 1) - 1;
            }

            void* ThreadLocal::Get0()
            {
                pthread_once(&tlsKeyInit, AllocateTlsKey);

                return pthread_getspecific(tlsKey);
            }

            void ThreadLocal::Set0(void* ptr)
            {
                pthread_once(&tlsKeyInit, AllocateTlsKey);

                pthread_setspecific(tlsKey, ptr);
            }
        }
    }
}
