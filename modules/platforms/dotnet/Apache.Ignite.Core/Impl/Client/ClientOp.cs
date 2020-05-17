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
    using Apache.Ignite.Core.Impl.Client.Transactions;

    /// <summary>
    /// Client op code.
    /// </summary>
    internal enum ClientOp : short
    {
        // General purpose.
        ResourceClose = 0,

        // Cache.
        [TransactionalOperation]
        CacheGet = 1000,
        [TransactionalOperation]
        CachePut = 1001,
        [TransactionalOperation]
        CachePutIfAbsent = 1002,
        [TransactionalOperation]
        CacheGetAll = 1003,
        [TransactionalOperation]
        CachePutAll = 1004,
        [TransactionalOperation]
        CacheGetAndPut = 1005,
        [TransactionalOperation]
        CacheGetAndReplace = 1006,
        [TransactionalOperation]
        CacheGetAndRemove = 1007,
        [TransactionalOperation]
        CacheGetAndPutIfAbsent = 1008,
        [TransactionalOperation]
        CacheReplace = 1009,
        [TransactionalOperation]
        CacheReplaceIfEquals = 1010,
        [TransactionalOperation]
        CacheContainsKey = 1011,
        [TransactionalOperation]
        CacheContainsKeys = 1012,
        [TransactionalOperation]
        CacheClear = 1013,
        [TransactionalOperation]
        CacheClearKey = 1014,
        [TransactionalOperation]
        CacheClearKeys = 1015,
        [TransactionalOperation]
        CacheRemoveKey = 1016,
        [TransactionalOperation]
        CacheRemoveIfEquals = 1017,
        [TransactionalOperation]
        CacheRemoveKeys = 1018,
        [TransactionalOperation]
        CacheRemoveAll = 1019,
        CacheGetSize = 1020,
        CacheGetNames = 1050,
        CacheCreateWithName = 1051,
        CacheGetOrCreateWithName = 1052,
        CacheCreateWithConfiguration = 1053,
        CacheGetOrCreateWithConfiguration = 1054,
        CacheGetConfiguration = 1055,
        CacheDestroy = 1056,
        
        [MinVersion(1, 4, 0)]
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

        // Transactions
        [MinVersion(1, 5, 0)]
        TxStart = 4000,
        [MinVersion(1, 5, 0)]
        TxEnd = 4001,

        // Cluster.
        [MinVersion(1, 5, 0)]
        ClusterIsActive = 5000,
        
        [MinVersion(1, 5, 0)]
        ClusterChangeState = 5001,
        
        [MinVersion(1, 5, 0)]
        ClusterChangeWalState = 5002,
        
        [MinVersion(1, 5, 0)]
        ClusterGetWalState = 5003,
        [MinVersion(1, 5, 0)]
        ClusterGroupGetNodeIds = 5100,
        [MinVersion(1, 5, 0)]
        ClusterGroupGetNodesInfo = 5101
    }
}
