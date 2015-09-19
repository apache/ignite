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

#ifndef _IGNITE_PORTABLE_CONSTS
#define _IGNITE_PORTABLE_CONSTS

#include <ignite/common/common.h>

namespace ignite 
{
    namespace portable 
    {
        /**
         * Portable collection types.
         */
        enum CollectionType 
        {
            /** 
             * Undefined. Maps to ArrayList in Java.
             */
            IGNITE_COLLECTION_UNDEFINED = 0,

            /** 
             * Array list. Maps to ArrayList in Java.
             */
            IGNITE_COLLECTION_ARRAY_LIST = 1,
            
            /**
             * Linked list. Maps to LinkedList in Java.
             */
            IGNITE_COLLECTION_LINKED_LIST = 2,
            
            /**
             * Hash set. Maps to HashSet in Java.
             */
            IGNITE_COLLECTION_HASH_SET = 3,
            
            /**
             * Linked hash set. Maps to LinkedHashSet in Java.
             */
            IGNITE_COLLECTION_LINKED_HASH_SET = 4,

            /**
             * Tree set. Maps to TreeSet in Java.
             */
            IGNITE_COLLECTION_TREE_SET = 5,

            /**
             * Concurrent skip list set. Maps to ConcurrentSkipListSet in Java.
             */
            IGNITE_COLLECTION_CONCURRENT_SKIP_LIST_SET = 6
        };

        /**
         * Portable map types.
         */
        enum MapType 
        {
            /**
             * Undefined. Maps to HashMap in Java.
             */
            IGNITE_MAP_UNDEFINED = 0,
            
            /**
             * Hash map. Maps to HashMap in Java.
             */
            IGNITE_MAP_HASH_MAP = 1,
            
            /**
             * Linked hash map. Maps to LinkedHashMap in Java.
             */
            IGNITE_MAP_LINKED_HASH_MAP = 2,

            /**
             * Tree map. Maps to TreeMap in Java.
             */
            IGNITE_MAP_TREE_MAP = 3,
            
            /**
             * Concurrent hash map. Maps to ConcurrentHashMap in Java.
             */
            IGNITE_MAP_CONCURRENT_HASH_MAP = 4,
            
            /**
             * Properties map. Maps to Properties in Java.
             */
            IGNITE_MAP_PROPERTIES_MAP = 5
        };
    }
}

#endif