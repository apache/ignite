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

#include "ignite/impl/handle_registry.h"

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        HandleRegistryEntry::~HandleRegistryEntry()
        {
            // No-op.
        }

        HandleRegistrySegment::HandleRegistrySegment() : 
            map(new std::map<int64_t, SharedPointer<HandleRegistryEntry>>()), mux(new CriticalSection())
        {
            // No-op.
        }

        HandleRegistrySegment::~HandleRegistrySegment()
        {
            delete map;
            delete mux;
        }

        SharedPointer<HandleRegistryEntry> HandleRegistrySegment::Get(int64_t hnd)
        {
            mux->Enter();

            SharedPointer<HandleRegistryEntry> res = (*map)[hnd];

            mux->Leave();

            return res;
        }

        void HandleRegistrySegment::Put(int64_t hnd, const SharedPointer<HandleRegistryEntry>& entry)
        {
            mux->Enter();

            (*map)[hnd] = entry;

            mux->Leave();
        }

        void HandleRegistrySegment::Remove(int64_t hnd)
        {
            mux->Enter();

            map->erase(hnd);

            mux->Leave();
        }

        void HandleRegistrySegment::Clear()
        {
            mux->Enter();

            map->erase(map->begin(), map->end());

            mux->Leave();
        }

        HandleRegistry::HandleRegistry(int32_t fastCap, int32_t slowSegmentCnt)
        {
            this->fastCap = fastCap;

            fastCtr = 0;

            fast = new SharedPointer<HandleRegistryEntry>[fastCap];

            for (int i = 0; i < fastCap; i++)
                fast[i] = SharedPointer<HandleRegistryEntry>();

            this->slowSegmentCnt = slowSegmentCnt;

            slowCtr = fastCap;

            slow = new HandleRegistrySegment*[slowSegmentCnt];

            for (int i = 0; i < slowSegmentCnt; i++)
                slow[i] = new HandleRegistrySegment();

            closed = 0;

            Memory::Fence();
        }

        HandleRegistry::~HandleRegistry()
        {
            Close();

            delete[] fast;

            for (int i = 0; i < slowSegmentCnt; i++)
                delete slow[i];

            delete[] slow;
        }

        int64_t HandleRegistry::Allocate(const SharedPointer<HandleRegistryEntry>& target)
        {
            return Allocate0(target, false, false);
        }

        int64_t HandleRegistry::AllocateCritical(const SharedPointer<HandleRegistryEntry>& target)
        {
            return Allocate0(target, true, false);
        }

        int64_t HandleRegistry::AllocateSafe(const SharedPointer<HandleRegistryEntry>& target)
        {
            return Allocate0(target, false, true);
        }

        int64_t HandleRegistry::AllocateCriticalSafe(const SharedPointer<HandleRegistryEntry>& target)
        {
            return Allocate0(target, true, true);
        }

        void HandleRegistry::Release(int64_t hnd)
        {
            if (hnd < fastCap)
                fast[static_cast<int32_t>(hnd)] = SharedPointer<HandleRegistryEntry>();
            else
            {
                HandleRegistrySegment* segment = *(slow + hnd % slowSegmentCnt);

                segment->Remove(hnd);
            }

            Memory::Fence();
        }

        SharedPointer<HandleRegistryEntry> HandleRegistry::Get(int64_t hnd)
        {
            Memory::Fence();

            if (hnd < fastCap)
                return fast[static_cast<int32_t>(hnd)];
            else
            {
                HandleRegistrySegment* segment = *(slow + hnd % slowSegmentCnt);

                return segment->Get(hnd);
            }
        }

        void HandleRegistry::Close()
        {
            if (Atomics::CompareAndSet32(&closed, 0, 1))
            {
                // Cleanup fast-path handles.
                for (int i = 0; i < fastCap; i++)
                    fast[i] = SharedPointer<HandleRegistryEntry>();

                // Cleanup slow-path handles.
                for (int i = 0; i < slowSegmentCnt; i++)
                    (*(slow + i))->Clear();
            }
        }

        int64_t HandleRegistry::Allocate0(const SharedPointer<HandleRegistryEntry>& target, bool critical, bool safe)
        {
            // Check closed state.
            Memory::Fence();

            if (closed == 1)
                return -1;

            // Try allocating entry on critical path.
            if (critical)
            {
                if (fastCtr < fastCap)
                {
                    int32_t fastIdx = Atomics::IncrementAndGet32(&fastCtr) - 1;

                    if (fastIdx < fastCap)
                    {
                        fast[fastIdx] = target;

                        // Double-check for closed state if safe mode is on.
                        Memory::Fence();

                        if (safe && closed == 1)
                        {
                            fast[fastIdx] = SharedPointer<HandleRegistryEntry>();

                            return -1;
                        }
                        else
                            return fastIdx;
                    }
                }
            }

            // Either allocating on slow-path, or fast-path can no longer accomodate more entries.
            int64_t slowIdx = Atomics::IncrementAndGet64(&slowCtr) - 1;

            HandleRegistrySegment* segment = *(slow + slowIdx % slowSegmentCnt);

            segment->Put(slowIdx, target);

            // Double-check for closed state if safe mode is on.
            Memory::Fence();

            if (safe && closed == 1)
            {
                segment->Remove(slowIdx);

                return -1;
            }

            return slowIdx;
        }
    }
}