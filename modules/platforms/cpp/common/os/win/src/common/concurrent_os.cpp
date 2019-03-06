/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

#include "ignite/common/concurrent_os.h"

#pragma intrinsic(_InterlockedCompareExchange64)

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

            CriticalSection::CriticalSection() :
                hnd()
            {
                InitializeCriticalSection(&hnd);

                Memory::Fence();
            }

            CriticalSection::~CriticalSection()
            {
                // No-op.
            }

            void CriticalSection::Enter()
            {
                Memory::Fence();

                EnterCriticalSection(&hnd);
            }

            void CriticalSection::Leave()
            {
                Memory::Fence();

                LeaveCriticalSection(&hnd);
            }

            SingleLatch::SingleLatch() :
                hnd(CreateEvent(NULL, TRUE, FALSE, NULL))
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
                return _InterlockedCompareExchange64(reinterpret_cast<LONG64*>(ptr), newVal, expVal);
            }

            int64_t Atomics::IncrementAndGet64(int64_t* ptr)
            {
#ifdef _WIN64
                return InterlockedIncrement64(reinterpret_cast<LONG64*>(ptr));
#else 
                while (true)
                {
                    int64_t expVal = *ptr;
                    int64_t newVal = expVal + 1;

                    if (CompareAndSet64(ptr, expVal, newVal))
                        return newVal;
                }
#endif
            }

            int64_t Atomics::DecrementAndGet64(int64_t* ptr)
            {
#ifdef _WIN64
                return InterlockedDecrement64(reinterpret_cast<LONG64*>(ptr));
#else 
                while (true)
                {
                    int64_t expVal = *ptr;
                    int64_t newVal = expVal - 1;

                    if (CompareAndSet64(ptr, expVal, newVal))
                        return newVal;
                }
#endif
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