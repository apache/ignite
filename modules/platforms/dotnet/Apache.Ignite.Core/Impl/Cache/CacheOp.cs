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
        LocPromote = 18,
        LocalClear = 20,
        LocalClearAll = 21,
        Lock = 22,
        LockAll = 23,
        Metrics = 24,
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
        Replace3 = 38
    }
}