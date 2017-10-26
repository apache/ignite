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

namespace Apache.Ignite.Core.Impl.Client
{
    /// <summary>
    /// Client op code.
    /// </summary>
    internal enum ClientOp : short
    {
        CacheGet = 1,
        BinaryTypeNameGet = 2,
        BinaryTypeGet = 3,
        CachePut = 4,
        BinaryTypeNamePut = 5,
        BinaryTypePut = 6,
        QueryScan = 7,
        QueryScanCursorGetPage = 8,
        ResourceClose = 9,
        CacheContainsKey = 10,
        CacheContainsKeys = 11,
        CacheGetAll = 12,
        CacheGetAndPut = 13,
        CacheGetAndReplace = 14,
        CacheGetAndRemove = 15,
        CachePutIfAbsent = 16,
        CacheGetAndPutIfAbsent = 17,
        CacheReplace = 18,
        CacheReplaceIfEquals = 19,
        CachePutAll = 20,
        CacheClear = 21,
        CacheClearKey = 22,
        CacheClearKeys = 23,
        CacheRemoveKey = 24,
        CacheRemoveIfEquals = 25,
        CacheGetSize = 26,
        CacheRemoveKeys = 27,
        CacheRemoveAll = 28,
        CacheCreateWithName = 29,
        CacheGetOrCreateWithName = 30,
        CacheDestroy = 31,
        CacheGetNames = 32,
        CacheGetConfiguration = 33,
        CacheCreateWithConfiguration = 34,
        CacheGetOrCreateWithConfiguration = 35
    }
}
