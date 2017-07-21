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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using Apache.Ignite.Core.Cache;

    /// <summary>
    /// Cache atomicity mode.
    /// </summary>
    public enum CacheAtomicityMode
    {
        /// <summary>
        /// Specifies fully ACID-compliant transactional cache behavior.
        /// </summary>
        Transactional,

        /// <summary>
        /// Specifies atomic-only cache behaviour. In this mode distributed transactions and distributed
        /// locking are not supported. Disabling transactions and locking allows to achieve much higher
        /// performance and throughput ratios.
        /// <para/>
        /// In addition to transactions and locking, one of the main differences to <see cref="Atomic"/> mode
        /// is that bulk writes, such as <see cref="ICache{TK,TV}.PutAll"/> 
        /// and <see cref="ICache{TK,TV}.RemoveAll(System.Collections.Generic.IEnumerable{TK})"/> methods, 
        /// become simple batch operations which can partially fail. In case of partial
        /// failure, <see cref="CachePartialUpdateException"/>will be thrown which will contain a list of keys 
        /// for which the update failed. It is recommended that bulk writes are used
        /// whenever multiple keys need to be inserted or updated in cache, as they reduce number of network trips and
        /// provide better performance.
        /// <para/>
        /// Note that even without locking and transactions, <see cref="Atomic"/> mode still provides
        /// full consistency guarantees across all cache nodes.
        /// <para/>
        /// Also note that all data modifications in <see cref="Atomic"/> mode are guaranteed to be atomic
        /// and consistent with writes to the underlying persistent store, if one is configured.        
        /// </summary>
        Atomic
    }
}