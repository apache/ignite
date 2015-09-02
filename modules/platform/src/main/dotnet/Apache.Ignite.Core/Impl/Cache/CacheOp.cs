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

namespace Apache.Ignite.Core.Impl.Cache
{
    /// <summary>
    /// Cache opcodes.
    /// </summary>
    internal enum CacheOp
    {
        CLEAR = 1,
        CLEAR_ALL = 2,
        CONTAINS_KEY = 3,
        CONTAINS_KEYS = 4,
        GET = 5,
        GET_ALL = 6,
        GET_AND_PUT = 7,
        GET_AND_PUT_IF_ABSENT = 8,
        GET_AND_REMOVE = 9,
        GET_AND_REPLACE = 10,
        GET_NAME = 11,
        INVOKE = 12,
        INVOKE_ALL = 13,
        IS_LOCAL_LOCKED = 14,
        LOAD_CACHE = 15,
        LOC_EVICT = 16,
        LOC_LOAD_CACHE = 17,
        LOC_PROMOTE = 18,
        LOCAL_CLEAR = 20,
        LOCAL_CLEAR_ALL = 21,
        LOCK = 22,
        LOCK_ALL = 23,
        METRICS = 24,
        PEEK = 25,
        PUT = 26,
        PUT_ALL = 27,
        PUT_IF_ABSENT = 28,
        QRY_CONTINUOUS = 29,
        QRY_SCAN = 30,
        QRY_SQL = 31,
        QRY_SQL_FIELDS = 32,
        QRY_TXT = 33,
        REMOVE_ALL = 34,
        REMOVE_BOOL = 35,
        REMOVE_OBJ = 36,
        REPLACE_2 = 37,
        REPLACE_3 = 38
    }
}