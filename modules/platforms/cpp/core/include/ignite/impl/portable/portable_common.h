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

#ifndef _IGNITE_IMPL_PORTABLE_COMMON
#define _IGNITE_IMPL_PORTABLE_COMMON

#include <stdint.h>

namespace ignite
{    
    namespace impl
    {
        namespace portable
        {
            /** Header: null. */
            const int8_t IGNITE_HDR_NULL = 101;

            /** Header: handle. */
            const int8_t IGNITE_HDR_HND = 102;

            /** Header: fulle form. */
            const int8_t IGNITE_HDR_FULL = 103;

            /** Full header length. */
            const int32_t IGNITE_FULL_HDR_LEN = 18;

            /** Type: object. */
            const int8_t IGNITE_TYPE_OBJECT = IGNITE_HDR_FULL;

            /** Type: unsigned byte. */
            const int8_t IGNITE_TYPE_BYTE = 1;

            /** Type: short. */
            const int8_t IGNITE_TYPE_SHORT = 2;

            /** Type: int. */
            const int8_t IGNITE_TYPE_INT = 3;

            /** Type: long. */
            const int8_t IGNITE_TYPE_LONG = 4;

            /** Type: float. */
            const int8_t IGNITE_TYPE_FLOAT = 5;

            /** Type: double. */
            const int8_t IGNITE_TYPE_DOUBLE = 6;

            /** Type: char. */
            const int8_t IGNITE_TYPE_CHAR = 7;

            /** Type: boolean. */
            const int8_t IGNITE_TYPE_BOOL = 8;

            /** Type: decimal. */
            const int8_t IGNITE_TYPE_DECIMAL = 30;

            /** Type: string. */
            const int8_t IGNITE_TYPE_STRING = 9;

            /** Type: UUID. */
            const int8_t IGNITE_TYPE_UUID = 10;

            /** Type: date. */
            const int8_t IGNITE_TYPE_DATE = 11;

            /** Type: unsigned byte array. */
            const int8_t IGNITE_TYPE_ARRAY_BYTE = 12;

            /** Type: short array. */
            const int8_t IGNITE_TYPE_ARRAY_SHORT = 13;

            /** Type: int array. */
            const int8_t IGNITE_TYPE_ARRAY_INT = 14;

            /** Type: long array. */
            const int8_t IGNITE_TYPE_ARRAY_LONG = 15;

            /** Type: float array. */
            const int8_t IGNITE_TYPE_ARRAY_FLOAT = 16;

            /** Type: double array. */
            const int8_t IGNITE_TYPE_ARRAY_DOUBLE = 17;

            /** Type: char array. */
            const int8_t IGNITE_TYPE_ARRAY_CHAR = 18;

            /** Type: boolean array. */
            const int8_t IGNITE_TYPE_ARRAY_BOOL = 19;

            /** Type: decimal array. */
            const int8_t IGNITE_TYPE_ARRAY_DECIMAL = 31;

            /** Type: string array. */
            const int8_t IGNITE_TYPE_ARRAY_STRING = 20;

            /** Type: UUID array. */
            const int8_t IGNITE_TYPE_ARRAY_UUID = 21;

            /** Type: date array. */
            const int8_t IGNITE_TYPE_ARRAY_DATE = 22;

            /** Type: object array. */
            const int8_t IGNITE_TYPE_ARRAY = 23;

            /** Type: collection. */
            const int8_t IGNITE_TYPE_COLLECTION = 24;

            /** Type: map. */
            const int8_t IGNITE_TYPE_MAP = 25;

            /** Type: map entry. */
            const int8_t IGNITE_TYPE_MAP_ENTRY = 26;

            /** Type: portable object. */
            const int8_t IGNITE_TYPE_PORTABLE = 27;

            /** Read/write single object. */
            const int32_t IGNITE_PORTABLE_MODE_SINGLE = 0;

            /** Read/write array. */
            const int32_t IGNITE_PORTABLE_MODE_ARRAY = 1;

            /** Read/write collection. */
            const int32_t IGNITE_PORTABLE_MODE_COL = 2;

            /** Read/write map. */
            const int32_t IGNITE_PORTABLE_MODE_MAP = 3;
        }
    }    
}

#endif