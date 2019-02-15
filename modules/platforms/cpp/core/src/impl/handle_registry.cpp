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

#include "ignite/impl/handle_registry.h"

using namespace ignite::common::concurrent;

namespace ignite
{
    namespace impl
    {
        HandleRegistrySegment::HandleRegistrySegment() : 
            map(),
            mux()
        {
            // No-op.
        }

        HandleRegistrySegment::~HandleRegistrySegment()
        {
            // No-op.
        }

        SharedPointer<void> HandleRegistrySegment::Get(int64_t hnd)
        {
            typedef std::map<int64_t, SharedPointer<void> > Map;

            CsLockGuard guard(mux);

            Map::const_iterator it = map.find(hnd);
            if (it == map.end())
                return SharedPointer<void>();

            return it->second;
        }

        void HandleRegistrySegment::Put(int64_t hnd, const SharedPointer<void>& entry)
        {
            CsLockGuard guard(mux);

            map[hnd] = entry;
        }

        void HandleRegistrySegment::Remove(int64_t hnd)
        {
            CsLockGuard guard(mux);

            map.erase(hnd);
        }

        void HandleRegistrySegment::Clear()
        {
            CsLockGuard guard(mux);

            map.clear();
        }

        HandleRegistry::HandleRegistry(int32_t fastCap, int32_t slowSegmentCnt) :
            fastCap(fastCap),
            fastCtr(0),
            fast(new SharedPointer<void>[fastCap]),
            slowSegmentCnt(slowSegmentCnt),
            slowCtr(fastCap),
            slow(new HandleRegistrySegment*[slowSegmentCnt]),
            closed(0)
        {
            for (int32_t i = 0; i < fastCap; i++)
                fast[i] = SharedPointer<void>();

            for (int32_t i = 0; i < slowSegmentCnt; i++)
                slow[i] = new HandleRegistrySegment();

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

        int64_t HandleRegistry::Allocate(const SharedPointer<void>& target)
        {
            return Allocate0(target, false, false);
        }

        int64_t HandleRegistry::AllocateCritical(const SharedPointer<void>& target)
        {
            return Allocate0(target, true, false);
        }

        int64_t HandleRegistry::AllocateSafe(const SharedPointer<void>& target)
        {
            return Allocate0(target, false, true);
        }

        int64_t HandleRegistry::AllocateCriticalSafe(const SharedPointer<void>& target)
        {
            return Allocate0(target, true, true);
        }

        void HandleRegistry::Release(int64_t hnd)
        {
            if (hnd < fastCap)
                fast[static_cast<int32_t>(hnd)] = SharedPointer<void>();
            else
            {
                HandleRegistrySegment* segment = slow[hnd % slowSegmentCnt];

                segment->Remove(hnd);
            }

            Memory::Fence();
        }

        SharedPointer<void> HandleRegistry::Get(int64_t hnd)
        {
            Memory::Fence();

            if (hnd < fastCap)
                return fast[static_cast<int32_t>(hnd)];
            else
            {
                HandleRegistrySegment* segment = slow[hnd % slowSegmentCnt];

                return segment->Get(hnd);
            }
        }

        void HandleRegistry::Close()
        {
            if (Atomics::CompareAndSet32(&closed, 0, 1))
            {
                // Cleanup fast-path handles.
                for (int32_t i = 0; i < fastCap; i++)
                    fast[i] = SharedPointer<void>();

                // Cleanup slow-path handles.
                for (int32_t i = 0; i < slowSegmentCnt; i++)
                    slow[i]->Clear();
            }
        }

        int64_t HandleRegistry::Allocate0(const SharedPointer<void>& target, bool critical, bool safe)
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
                            fast[fastIdx] = SharedPointer<void>();

                            return -1;
                        }
                        else
                            return fastIdx;
                    }
                }
            }

            // Either allocating on slow-path, or fast-path can no longer accomodate more entries.
            int64_t slowIdx = Atomics::IncrementAndGet64(&slowCtr) - 1;

            HandleRegistrySegment* segment = slow[slowIdx % slowSegmentCnt];

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