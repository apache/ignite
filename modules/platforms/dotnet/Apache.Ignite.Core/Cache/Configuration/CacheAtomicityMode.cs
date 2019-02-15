/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        /// <para />
        /// <b>Note!</b> In this mode, transactional consistency is guaranteed for key-value API operations only.
        /// To enable ACID capabilities for SQL transactions, use TRANSACTIONAL_SNAPSHOT mode.
        /// <para />
        /// <b>Note!</b> This atomicity mode is not compatible with the other atomicity modes within the same transaction.
        /// If a transaction is executed over multiple caches, all caches must have the same atomicity mode,
        /// either TRANSACTIONAL_SNAPSHOT or TRANSACTIONAL.
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
        Atomic,

        /// <summary>
        /// Specifies fully ACID-compliant transactional cache behavior for both key-value API and SQL transactions.
        /// <para/>
        /// This atomicity mode enables multiversion concurrency control (MVCC) for the cache. In MVCC-enabled caches,
        /// when a transaction updates a row, it creates a new version of that row instead of overwriting it.
        /// Other users continue to see the old version of the row until the transaction is committed.
        /// In this way, readers and writers do not conflict with each other and always work with a consistent dataset.
        /// The old version of data is cleaned up when it's no longer accessed by anyone.
        /// <para />
        /// With this mode enabled, one node is elected as an MVCC coordinator. This node tracks all in-flight transactions
        /// and queries executed in the cluster. Each transaction or query executed over the cache with
        /// TRANSACTIONAL_SNAPSHOT mode works with a current snapshot of data generated for this transaction or query
        /// by the coordinator. This snapshot ensures that the transaction works with a consistent database state
        /// during its execution period.
        /// <para />
        /// <b>Note!</b> This atomicity mode is not compatible with the other atomicity modes within the same transaction.
        /// If a transaction is executed over multiple caches, all caches must have the same atomicity mode,
        /// either TRANSACTIONAL_SNAPSHOT or TRANSACTIONAL.
        /// </summary>
        TransactionalSnapshot,
    }
}