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

/**
 * @file
 * Declares ignite::cache::CachePeekMode enum.
 */

#ifndef _IGNITE_CACHE_CACHE_PEEK_MODE
#define _IGNITE_CACHE_CACHE_PEEK_MODE

namespace ignite
{
    namespace cache
    {
        /**
         * Enumeration of all supported cache peek modes.
         */
        enum CachePeekMode
        {
            /**
             * Peeks into all available cache storages.
             */
            IGNITE_PEEK_MODE_ALL = 0x01,

            /**
             * Peek into near cache only (don't peek into partitioned cache).
             * In case of LOCAL cache, behaves as IGNITE_PEEK_MODE_ALL mode.
             */
            IGNITE_PEEK_MODE_NEAR = 0x02,

            /**
             * Peek value from primary copy of partitioned cache only (skip near cache).
             * In case of LOCAL cache, behaves as IGNITE_PEEK_MODE_ALL mode.
             */
            IGNITE_PEEK_MODE_PRIMARY = 0x04,

            /**
             * Peek value from backup copies of partitioned cache only (skip near cache).
             * In case of LOCAL cache, behaves as IGNITE_PEEK_MODE_ALL mode.
             */
            IGNITE_PEEK_MODE_BACKUP = 0x08,

            /**
             * Peeks value from the on-heap storage only.
             */
            IGNITE_PEEK_MODE_ONHEAP = 0x10,

            /**
             * Peeks value from the off-heap storage only, without loading off-heap value into cache.
             */
            IGNITE_PEEK_MODE_OFFHEAP = 0x20,

            /**
             * Peeks value from the swap storage only, without loading swapped value into cache.
             */
            IGNITE_PEEK_MODE_SWAP = 0x40
        };
    }
}

#endif //_IGNITE_CACHE_CACHE_PEEK_MODE