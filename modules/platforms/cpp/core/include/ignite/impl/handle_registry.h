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

#ifndef _IGNITE_IMPL_HANDLE_REGISTRY
#define _IGNITE_IMPL_HANDLE_REGISTRY

#include <map>
#include <stdint.h>

#include <ignite/common/concurrent.h>

namespace ignite
{
    namespace impl
    {
        /**
         * Handle registry segment containing thread-specific data for slow-path access.
         */
        class HandleRegistrySegment
        {
        public:
            /**
             * Constructor.
             */
            HandleRegistrySegment();

            /**
             * Destructor.
             */
            ~HandleRegistrySegment();

            /**
             * Get entry from segment.
             *
             * @param hnd Handle.
             * @return Associated entry or NULL if it doesn't exists.
             */
            common::concurrent::SharedPointer<void> Get(int64_t hnd);

            /**
             * Put entry into segment.
             *
             * @param hnd Handle.
             * @param entry Associated entry (cannot be NULL).
             */
            void Put(int64_t hnd, const common::concurrent::SharedPointer<void>& entry);

            /**
             * Remove entry from the segment.
             *
             * @param hnd Handle.
             */
            void Remove(int64_t hnd);

            /**
             * Clear all entries from the segment.
             */
            void Clear();
        private:
            /** Map with data. */
            std::map<int64_t, common::concurrent::SharedPointer<void> > map;

            /** Mutex. */
            common::concurrent::CriticalSection mux;

            IGNITE_NO_COPY_ASSIGNMENT(HandleRegistrySegment);
        };

        /**
         * Handle registry.
         */
        class IGNITE_IMPORT_EXPORT HandleRegistry
        {
        public:
            /**
             * Constructor.
             *
             * @param fastCap Fast-path capacity.
             * @param slowSegmentCnt Slow-path segments count.
             */
            HandleRegistry(int32_t fastCap, int32_t slowSegmentCnt);

            /**
             * Destructor.
             */
            ~HandleRegistry();

            /**
             * Allocate handle.
             *
             * @param target Target.
             * @return Handle.
             */
            int64_t Allocate(const common::concurrent::SharedPointer<void>& target);

            /**
             * Allocate handle in critical mode.
             *
             * @param target Target.
             * @return Handle.
             */
            int64_t AllocateCritical(const common::concurrent::SharedPointer<void>& target);

            /**
             * Allocate handle in safe mode.
             *
             * @param target Target.
             * @return Handle.
             */
            int64_t AllocateSafe(const common::concurrent::SharedPointer<void>& target);

            /**
             * Allocate handle in critical and safe modes.
             *
             * @param target Target.
             * @return Handle.
             */
            int64_t AllocateCriticalSafe(const common::concurrent::SharedPointer<void>& target);

            /**
             * Release handle.
             *
             * @param hnd Handle.
             */
            void Release(int64_t hnd);

            /**
             * Get target.
             *
             * @param hnd Handle.
             * @return Target.
             */
            common::concurrent::SharedPointer<void> Get(int64_t hnd);

            /**
             * Close the registry.
             */
            void Close();

        private:
            /** Fast-path container capacity. */
            int32_t fastCap;

            /** Fast-path counter. */
            int32_t fastCtr;

            /** Fast-path container. */
            common::concurrent::SharedPointer<void>* fast;

            /** Amount of slow-path segments. */
            int32_t slowSegmentCnt;

            /** Slow-path counter. */
            int64_t slowCtr;

            /** Slow-path segments. */
            HandleRegistrySegment** slow;

            /** Close flag. */
            int32_t closed;

            IGNITE_NO_COPY_ASSIGNMENT(HandleRegistry);

            /**
             * Internal allocation routine.
             *
             * @param target Target.
             * @param critical mode flag.
             * @param safe mode flag.
             */
            int64_t Allocate0(const common::concurrent::SharedPointer<void>& target, bool critical, bool safe);
        };
    }
}

#endif //_IGNITE_IMPL_HANDLE_REGISTRY