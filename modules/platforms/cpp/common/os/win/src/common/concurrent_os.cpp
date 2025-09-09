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

            ReadWriteLock::ReadWriteLock() :
                lock()
            {
                InitializeSRWLock(&lock);

                Memory::Fence();
            }

            ReadWriteLock::~ReadWriteLock()
            {
                // No-op.
            }

            void ReadWriteLock::LockExclusive()
            {
                AcquireSRWLockExclusive(&lock);
            }

            void ReadWriteLock::ReleaseExclusive()
            {
                ReleaseSRWLockExclusive(&lock);
            }

            void ReadWriteLock::LockShared()
            {
                AcquireSRWLockShared(&lock);
            }

            void ReadWriteLock::ReleaseShared()
            {
                ReleaseSRWLockShared(&lock);
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

            Thread::Thread() :
                handle(NULL)
            {
                // No-op.
            }

            Thread::~Thread()
            {
                if (handle)
                    CloseHandle(handle);
            }

            DWORD Thread::ThreadRoutine(LPVOID lpParam)
            {
                Thread* self = static_cast<Thread*>(lpParam);

                self->Run();

                return 0;
            }

            void Thread::Start()
            {
                handle = CreateThread(NULL, 0, Thread::ThreadRoutine, this, 0, NULL);

                assert(handle != NULL);
            }

            void Thread::Join()
            {
                WaitForSingleObject(handle, INFINITE);
            }

            uint32_t GetNumberOfProcessors()
            {
                SYSTEM_INFO info;
                GetSystemInfo(&info);

                return static_cast<uint32_t>(info.dwNumberOfProcessors < 0 ? 0 : info.dwNumberOfProcessors);
            }

            int32_t GetThreadsCount()
            {
                DWORD id = GetCurrentProcessId();
                HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPALL, 0);

                PROCESSENTRY32 entry;
                memset(&entry, 0, sizeof(entry));
                entry.dwSize = sizeof(entry);

                BOOL ret = Process32First(snapshot, &entry);

                while (ret && entry.th32ProcessID != id)
                    ret = Process32Next(snapshot, &entry);

                CloseHandle(snapshot);
                return static_cast<int32_t>(ret ? entry.cntThreads : -1);
            }
        }
    }
}
