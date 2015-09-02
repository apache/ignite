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
            /** Thread-local index for Windows. */
            DWORD winTlsIdx;

            void Memory::Fence() {
                MemoryBarrier();
            }

            CriticalSection::CriticalSection() : hnd(new CRITICAL_SECTION) {
                InitializeCriticalSection(hnd);

                Memory::Fence();
            }

            CriticalSection::~CriticalSection() {
                Memory::Fence();

                delete hnd;
            }

            void CriticalSection::Enter() {
                Memory::Fence();

                EnterCriticalSection(hnd);
            }

            void CriticalSection::Leave() {
                Memory::Fence();

                LeaveCriticalSection(hnd);
            }

            SingleLatch::SingleLatch() : hnd(CreateEvent(NULL, TRUE, FALSE, NULL))
            {
                Memory::Fence();
            }

            SingleLatch::~SingleLatch()
            {
                Memory::Fence();

                CloseHandle(hnd);
            }

            void SingleLatch::CountDown()
            {
                SetEvent(hnd);
            }

            void SingleLatch::Await()
            {
                WaitForSingleObject(hnd, INFINITE);
            }

            bool Atomics::CompareAndSet32(int32_t* ptr, int32_t expVal, int32_t newVal)
            {
                return CompareAndSet32Val(ptr, expVal, newVal) == expVal;
            }

            int32_t Atomics::CompareAndSet32Val(int32_t* ptr, int32_t expVal, int32_t newVal)
            {
                return InterlockedCompareExchange(reinterpret_cast<LONG*>(ptr), newVal, expVal);
            }

            int32_t Atomics::IncrementAndGet32(int32_t* ptr)
            {
                return InterlockedIncrement(reinterpret_cast<LONG*>(ptr));
            }

            int32_t Atomics::DecrementAndGet32(int32_t* ptr)
            {
                return InterlockedDecrement(reinterpret_cast<LONG*>(ptr));
            }

            bool Atomics::CompareAndSet64(int64_t* ptr, int64_t expVal, int64_t newVal)
            {
                return CompareAndSet64Val(ptr, expVal, newVal) == expVal;
            }

            int64_t Atomics::CompareAndSet64Val(int64_t* ptr, int64_t expVal, int64_t newVal)
            {
                return InterlockedCompareExchange64(reinterpret_cast<LONG64*>(ptr), newVal, expVal);
            }

            int64_t Atomics::IncrementAndGet64(int64_t* ptr)
            {
                return InterlockedIncrement64(reinterpret_cast<LONG64*>(ptr));
            }

            int64_t Atomics::DecrementAndGet64(int64_t* ptr)
            {
                return InterlockedDecrement64(reinterpret_cast<LONG64*>(ptr));
            }
            
            bool ThreadLocal::OnProcessAttach()
            {
                return (winTlsIdx = TlsAlloc()) != TLS_OUT_OF_INDEXES;
            }

            void ThreadLocal::OnThreadDetach()
            {
                if (winTlsIdx != TLS_OUT_OF_INDEXES)
                {
                    void* mapPtr = Get0();

                    Clear0(mapPtr);
                }
            }

            void ThreadLocal::OnProcessDetach()
            {
                if (winTlsIdx != TLS_OUT_OF_INDEXES)
                    TlsFree(winTlsIdx);
            }

            void* ThreadLocal::Get0()
            {
                return TlsGetValue(winTlsIdx);
            }

            void ThreadLocal::Set0(void* ptr)
            {
                TlsSetValue(winTlsIdx, ptr);
            }
        }
    }
}