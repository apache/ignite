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
        /// Remove entry from cache (<see cref="ICacheClient{TK,TV}.remove(Object)}, <see cref="ICacheClient{TK,TV}.remove(Object, Object)"/>).
        /// </summary>
        CacheRemoveOne,

        /// <summary>
        /// Remove entries from cache (<see cref="ICacheClient{TK,TV}.removeAll(Set)"/>).
        /// </summary>
        CacheRemoveMultiple,

        /// <summary>
        /// Remove everyting from cache (<see cref="ICacheClient{TK,TV}.removeAll()"/>).
        /// </summary>
        CacheRemoveEverything,

        /// <summary>
        /// Clear cache entry (<see cref="ICacheClient{TK,TV}.clear(Object)} ).
        /// </summary>
        CacheClearOne,

        /// <summary>
        /// Clear multiple cache entries (<see cref="ICacheClient{TK,TV}.clearAll(Set)"/>).
        /// </summary>
        CacheClearMultiple,

        /// <summary>
        /// Clear entire cache (<see cref="ICacheClient{TK,TV}.clear()"/>).
        /// </summary>
        CacheClearEverything,

        /// <summary>
        /// Get and put (<see cref="ICacheClient{TK,TV}.getAndPut(Object, Object)"/>).
        /// </summary>
        CacheGetAndPut,

        /// <summary>
        /// Get and remove (<see cref="ICacheClient{TK,TV}.getAndRemove(Object)"/>).
        /// </summary>
        CacheGetAndRemove,

        /// <summary>
        /// Get and replace (<see cref="ICacheClient{TK,TV}.getAndReplace(Object, Object)"/>).
        /// </summary>
        CacheGetAndReplace,

        /// <summary>
        /// Put if absent (<see cref="ICacheClient{TK,TV}.putIfAbsent(Object, Object)"/>).
        /// </summary>
        CachePutIfAbsent,

        /// <summary>
        /// Get and put if absent (<see cref="ICacheClient{TK,TV}.getAndPutIfAbsent(Object, Object)"/>).
        /// </summary>
        CacheGetAndPutIfAbsent,

        /// <summary>
        /// Scan query (<see cref="ICacheClient{TK,TV}.query(Query)"/>).
        /// </summary>
        QueryScan,

        /// <summary>
        /// SQL query (<see cref="ICacheClient{TK,TV}.query(SqlFieldsQuery)"/>).
        /// </summary>
        QuerySql,

        /// <summary>
        /// Continuous query (<see cref="ICacheClient{TK,TV}.query(ContinuousQuery, ClientDisconnectListener)"/>).
        /// </summary>
        QueryContinuous,

        /// <summary>
        /// Start transaction ({@link ClientTransactions#txStart"/>).
        /// </summary>
        TransactionStart,

        /// <summary>
        /// Get cluster state ({@link ClientCluster#state()"/>).
        /// </summary>
        ClusterGetState,

        /// <summary>
        /// Change cluster state ({@link ClientCluster#state(ClusterState)"/>).
        /// </summary>
        ClusterChangeState,

        /// <summary>
        /// Get cluster WAL state ({@link ClientCluster#isWalEnabled(String)"/>).
        /// </summary>
        ClusterGetWalState,

        /// <summary>
        /// Change cluster WAL state ({@link ClientCluster#enableWal(String)}, {@link ClientCluster#disableWal(String)"/>).
        /// </summary>
        ClusterChangeWalState,

        /// <summary>
        /// Get cluster nodes ({@link ClientCluster#nodes()"/>).
        /// </summary>
        ClusterGroupGetNodes,

        /// <summary>
        /// Execute compute task ({@link ClientCompute#execute(String, Object)"/>).
        /// </summary>
        ComputeTaskExecute,

        /// <summary>
        /// Invoke service.
        /// </summary>
        ServiceInvoke,

        /// <summary>
        /// Get service descriptors ({@link ClientServices#serviceDescriptors()"/>).
        /// </summary>
        ServiceGetDescriptors,

        /// <summary>
        /// Get service descriptor ({@link ClientServices#serviceDescriptor(String)"/>).
        /// </summary>
        ServiceGetDescriptor
    }
}
