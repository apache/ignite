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
        // General purpose.
        ResourceClose = 0,

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

        // Metadata.
        BinaryTypeNameGet = 3000,
        BinaryTypeNamePut = 3001,
        BinaryTypeGet = 3002,
        BinaryTypePut = 3003,

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
        ComputeTaskFinished = 6001
    }
}
