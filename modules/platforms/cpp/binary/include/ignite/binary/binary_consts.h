/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file
 * Declares specific binary constatants
 */

#ifndef _IGNITE_BINARY_BINARY_CONSTS
#define _IGNITE_BINARY_BINARY_CONSTS

namespace ignite 
{
    namespace binary 
    {
        /**
         * Binary collection types.
         */
        struct CollectionType 
        {
            enum Type
            {
                /**
                 * Undefined. Maps to ArrayList in Java.
                 */
                UNDEFINED = 0,

                /** 
                 * Array list. Maps to ArrayList in Java.
                 */
                ARRAY_LIST = 1,

                /**
                 * Linked list. Maps to LinkedList in Java.
                 */
                LINKED_LIST = 2,

                /**
                 * Hash set. Maps to HashSet in Java.
                 */
                HASH_SET = 3,

                /**
                 * Linked hash set. Maps to LinkedHashSet in Java.
                 */
                LINKED_HASH_SET = 4
            };
        };

        /**
         * Binary map types.
         */
        struct MapType 
        {
            enum Type
            {
                /**
                 * Undefined. Maps to HashMap in Java.
                 */
                UNDEFINED = 0,

                /**
                 * Hash map. Maps to HashMap in Java.
                 */
                HASH_MAP = 1,

                /**
                 * Linked hash map. Maps to LinkedHashMap in Java.
                 */
                LINKED_HASH_MAP = 2
            };
        };
    }
}

#endif //_IGNITE_BINARY_BINARY_CONSTS