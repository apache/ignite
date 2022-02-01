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

namespace Apache.Ignite.Core.Client
{
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Client.Transactions;

    /// <summary>
    /// Client operation type.
    /// </summary>
    public enum ClientOperationType
    {
        /// <summary>
        /// Create cache <see cref="IIgniteClient.CreateCache{TK,TV}(string)"/>,
        /// <see cref="IIgniteClient.CreateCache{TK,TV}(CacheClientConfiguration)"/>.
        /// </summary>
        CacheCreate,

        /// <summary>
        /// Get or create cache <see cref="IIgniteClient.GetOrCreateCache{TK,TV}(string)"/>,
        /// <see cref="IIgniteClient.GetOrCreateCache{TK,TV}(CacheClientConfiguration)"/>.
        /// </summary>
        CacheGetOrCreate,

        /// <summary>
        /// Get cache names <see cref="IIgniteClient.GetCacheNames"/>.
        /// </summary>
        CacheGetNames,

        /// <summary>
        /// Destroy cache <see cref="IIgniteClient.DestroyCache"/>.
        /// </summary>
        CacheDestroy,

        /// <summary>
        /// Get value from cache <see cref="ICacheClient{TK,TV}.Get"/>.
        /// </summary>
        CacheGet,

        /// <summary>
        /// Put value to cache <see cref="ICacheClient{TK,TV}.Put"/>.
        /// </summary>
        CachePut,

        /// <summary>
        /// Determines if the cache contains a key <see cref="ICacheClient{TK,TV}.Put"/>.
        /// </summary>
        CacheContainsKey,

        /// <summary>
        /// Determines if the cache contains multiple keys (<see cref="ICacheClient{TK,TV}.ContainsKeys"/>).
        /// </summary>
        CacheContainsKeys,

        /// <summary>
        /// Get cache configuration (<see cref="ICacheClient{TK,TV}.GetConfiguration"/>).
        /// </summary>
        CacheGetConfiguration,

        /// <summary>
        /// Get cache size (<see cref="ICacheClient{TK,TV}.GetSize"/>).
        /// </summary>
        CacheGetSize,

        /// <summary>
        /// Put values to cache (<see cref="ICacheClient{TK,TV}.PutAll"/>).
        /// </summary>
        CachePutAll,

        /// <summary>
        /// Get values from cache (<see cref="ICacheClient{TK,TV}.GetAll"/>).
        /// </summary>
        CacheGetAll,

        /// <summary>
        /// Replace cache value (<see cref="ICacheClient{TK,TV}.Replace(TK,TV)"/>,
        /// <see cref="ICacheClient{TK,TV}.Replace(TK,TV,TV)"/>).
        /// </summary>
        CacheReplace,

        /// <summary>
        /// Remove entry from cache (<see cref="ICacheClient{TK,TV}.Remove(TK)" />,
        /// <see cref="ICacheClient{TK,TV}.Remove(TK,TV)"/>).
        /// </summary>
        CacheRemoveOne,

        /// <summary>
        /// Remove entries from cache (<see cref="ICacheClient{TK,TV}.RemoveAll(System.Collections.Generic.IEnumerable{TK})"/>).
        /// </summary>
        CacheRemoveMultiple,

        /// <summary>
        /// Remove everything from cache (<see cref="ICacheClient{TK,TV}.RemoveAll()"/>).
        /// </summary>
        CacheRemoveEverything,

        /// <summary>
        /// Clear cache entry (<see cref="ICacheClient{TK,TV}.Clear(TK)"/>).
        /// </summary>
        CacheClearOne,

        /// <summary>
        /// Clear multiple cache entries (<see cref="ICacheClient{TK,TV}.ClearAll"/>).
        /// </summary>
        CacheClearMultiple,

        /// <summary>
        /// Clear entire cache (<see cref="ICacheClient{TK,TV}.Clear()"/>).
        /// </summary>
        CacheClearEverything,

        /// <summary>
        /// Get and put (<see cref="ICacheClient{TK,TV}.GetAndPut(TK, TV)"/>).
        /// </summary>
        CacheGetAndPut,

        /// <summary>
        /// Get and remove (<see cref="ICacheClient{TK,TV}.GetAndRemove(TK)"/>).
        /// </summary>
        CacheGetAndRemove,

        /// <summary>
        /// Get and replace (<see cref="ICacheClient{TK,TV}.GetAndReplace(TK, TV)"/>).
        /// </summary>
        CacheGetAndReplace,

        /// <summary>
        /// Put if absent (<see cref="ICacheClient{TK,TV}.PutIfAbsent(TK, TV)"/>).
        /// </summary>
        CachePutIfAbsent,

        /// <summary>
        /// Get and put if absent (<see cref="ICacheClient{TK,TV}.GetAndPutIfAbsent(TK, TV)"/>).
        /// </summary>
        CacheGetAndPutIfAbsent,

        /// <summary>
        /// Scan query (<see cref="ICacheClient{TK,TV}.Query(Apache.Ignite.Core.Cache.Query.ScanQuery{TK,TV})"/>).
        /// </summary>
        QueryScan,

        /// <summary>
        /// SQL query (<see cref="ICacheClient{TK,TV}.Query(Apache.Ignite.Core.Cache.Query.SqlFieldsQuery)"/>).
        /// </summary>
        QuerySql,

        /// <summary>
        /// Continuous query (<see cref="ICacheClient{TK,TV}.QueryContinuous"/>).
        /// </summary>
        QueryContinuous,

        /// <summary>
        /// Start transaction (<see cref="ITransactionsClient.TxStart()"/>).
        /// </summary>
        TransactionStart,

        /// <summary>
        /// Get cluster state (<see cref="IClientCluster.IsActive"/>).
        /// </summary>
        ClusterGetState,

        /// <summary>
        /// Change cluster state (<see cref="IClientCluster.SetActive"/>).
        /// </summary>
        ClusterChangeState,

        /// <summary>
        /// Get cluster WAL state (<see cref="IClientCluster.IsWalEnabled"/>).
        /// </summary>
        ClusterGetWalState,

        /// <summary>
        /// Change cluster WAL state (<see cref="IClientCluster.EnableWal"/>, <see cref="IClientCluster.DisableWal"/>).
        /// </summary>
        ClusterChangeWalState,

        /// <summary>
        /// Get cluster nodes (<see cref="IClientClusterGroup.GetNodes"/>).
        /// </summary>
        ClusterGroupGetNodes,

        /// <summary>
        /// Execute compute task (<see cref="IComputeClient.ExecuteJavaTask{TRes}"/>).
        /// </summary>
        ComputeTaskExecute,

        /// <summary>
        /// Invoke service.
        /// </summary>
        ServiceInvoke,

        /// <summary>
        /// Get service descriptors (<see cref="IServicesClient.GetServiceDescriptors"/>).
        /// </summary>
        ServiceGetDescriptors,

        /// <summary>
        /// Get service descriptor (<see cref="IServicesClient.GetServiceDescriptor"/>).
        /// </summary>
        ServiceGetDescriptor
    }
}
