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
        None = 0,
        Clear = 1,
        ClearAll = 2,
        ContainsKey = 3,
        ContainsKeys = 4,
        Get = 5,
        GetAll = 6,
        GetAndPut = 7,
        GetAndPutIfAbsent = 8,
        GetAndRemove = 9,
        GetAndReplace = 10,
        GetName = 11,
        Invoke = 12,
        InvokeAll = 13,
        IsLocalLocked = 14,
        LoadCache = 15,
        LocEvict = 16,
        LocLoadCache = 17,
        LocalClear = 20,
        LocalClearAll = 21,
        Lock = 22,
        LockAll = 23,
        LocalMetrics = 24,
        Peek = 25,
        Put = 26,
        PutAll = 27,
        PutIfAbsent = 28,
        QryContinuous = 29,
        QryScan = 30,
        QrySql = 31,
        QrySqlFields = 32,
        QryTxt = 33,
        RemoveAll = 34,
        RemoveBool = 35,
        RemoveObj = 36,
        Replace2 = 37,
        Replace3 = 38,
        GetConfig = 39,
        LoadAll = 40,
        ClearCache = 41,
        WithPartitionRecover = 42,
        RemoveAll2 = 43,
        WithKeepBinary = 44,
        WithExpiryPolicy = 45,
        WithNoRetries = 46,
        WithSkipStore = 47,
        Size = 48,
        Iterator = 49,
        LocIterator = 50,
        EnterLock = 51,
        ExitLock = 52,
        TryEnterLock = 53,
        CloseLock = 54,
        Rebalance = 55,
        SizeLoc = 56,
        PutAsync = 57,
        ClearCacheAsync = 58,
        ClearAllAsync = 59,
        RemoveAll2Async = 60,
        SizeAsync = 61,
        ClearAsync = 62,
        LoadCacheAsync = 63,
        LocLoadCacheAsync = 64,
        PutAllAsync = 65,
        RemoveAllAsync = 66,
        GetAsync = 67,
        ContainsKeyAsync = 68,
        ContainsKeysAsync = 69,
        RemoveBoolAsync = 70,
        RemoveObjAsync = 71,
        GetAllAsync = 72,
        GetAndPutAsync = 73,
        GetAndPutIfAbsentAsync = 74,
        GetAndRemoveAsync = 75,
        GetAndReplaceAsync = 76,
        Replace2Async = 77,
        Replace3Async = 78,
        InvokeAsync = 79,
        InvokeAllAsync = 80,
        PutIfAbsentAsync = 81,
        Extension = 82,
        GlobalMetrics = 83,
        GetLostPartitions = 84,
        QueryMetrics = 85,
        ResetQueryMetrics = 86,
        PreloadPartition = 87,
        PreloadPartitionAsync = 88,
        LocalPreloadPartition = 89,
        SizeLong = 90,
        SizeLongAsync = 91,
        SizeLongLoc = 92,
        EnableStatistics = 93
    }
}
