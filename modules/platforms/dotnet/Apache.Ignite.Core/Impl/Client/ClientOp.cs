﻿/*
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
        // General purpose.
        ResourceClose = 0,
        Heartbeat = 1,
        GetIdleTimeout = 2,

        // Cache.
        CacheGet = 1000,
        CachePut = 1001,
        CachePutIfAbsent = 1002,
        CacheGetAll = 1003,
        CachePutAll = 1004,
        CacheGetAndPut = 1005,
        CacheGetAndReplace = 1006,
        CacheGetAndRemove = 1007,
        CacheGetAndPutIfAbsent = 1008,
        CacheReplace = 1009,
        CacheReplaceIfEquals = 1010,
        CacheContainsKey = 1011,
        CacheContainsKeys = 1012,
        CacheClear = 1013,
        CacheClearKey = 1014,
        CacheClearKeys = 1015,
        CacheRemoveKey = 1016,
        CacheRemoveIfEquals = 1017,
        CacheRemoveKeys = 1018,
        CacheRemoveAll = 1019,
        CacheGetSize = 1020,
        CacheGetNames = 1050,
        CacheCreateWithName = 1051,
        CacheGetOrCreateWithName = 1052,
        CacheCreateWithConfiguration = 1053,
        CacheGetOrCreateWithConfiguration = 1054,
        CacheGetConfiguration = 1055,
        CacheDestroy = 1056,
        CachePartitions = 1101,

        // Queries.
        QueryScan = 2000,
        QueryScanCursorGetPage = 2001,
        QuerySql = 2002,
        QuerySqlCursorGetPage = 2003,
        QuerySqlFields = 2004,
        QuerySqlFieldsCursorGetPage = 2005,
        QueryContinuous = 2006,
        QueryContinuousEventNotification = 2007,

        // Metadata.
        BinaryTypeNameGet = 3000,
        BinaryTypeNamePut = 3001,
        BinaryTypeGet = 3002,
        BinaryTypePut = 3003,
        BinaryConfigurationGet = 3004,

        // Transactions
        TxStart = 4000,
        TxEnd = 4001,

        // Cluster.
        ClusterIsActive = 5000,
        ClusterChangeState = 5001,
        ClusterChangeWalState = 5002,
        ClusterGetWalState = 5003,
        ClusterGroupGetNodeIds = 5100,
        ClusterGroupGetNodesInfo = 5101,
        ClusterGroupGetNodesEndpoints = 5102,

        // Compute.
        ComputeTaskExecute = 6000,
        ComputeTaskFinished = 6001,

        // Services.
        ServiceInvoke = 7000,
        ServiceGetDescriptors = 7001,
        ServiceGetDescriptor = 7002,

        // Data Streamer.
        DataStreamerStart = 8000,
        DataStreamerAddData = 8001,

        // Data Structures.
        AtomicLongCreate = 9000,
        AtomicLongRemove = 9001,
        AtomicLongExists = 9002,
        AtomicLongValueGet = 9003,
        AtomicLongValueAddAndGet = 9004,
        AtomicLongValueGetAndSet = 9005,
        AtomicLongValueCompareAndSet = 9006,
        AtomicLongValueCompareAndSetAndGet = 9007,

        SetGetOrCreate = 9010,
        SetClose = 9011,
        SetExists = 9012,
        SetValueAdd = 9013,
        SetValueAddAll = 9014,
        SetValueRemove = 9015,
        SetValueRemoveAll = 9016,
        SetValueContains = 9017,
        SetValueContainsAll = 9018,
        SetValueRetainAll = 9019,
        SetSize = 9020,
        SetClear = 9021,
        SetIteratorStart = 9022,
        SetIteratorGetPage = 9023
    }
}
