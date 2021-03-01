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
