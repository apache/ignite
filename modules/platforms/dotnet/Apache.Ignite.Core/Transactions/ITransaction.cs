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

namespace Apache.Ignite.Core.Transactions
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Grid cache transaction. 
    /// <para />
    /// Cache transactions support the following isolation levels:
    /// <list type="bullet">
    ///     <item>
    ///         <description><see cref="TransactionIsolation.ReadCommitted"/> isolation level 
    ///         means that always a committed value will be provided for read operations. With this isolation 
    ///         level values are always read from cache global memory or persistent store every time a value 
    ///         is accessed. In other words, if the same key is accessed more than once within the same transaction, 
    ///         it may have different value every time since global cache memory may be updated concurrently by 
    ///         other threads.</description>
    ///     </item>
    ///     <item>
    ///         <description><see cref="TransactionIsolation.RepeatableRead"/> isolation level 
    ///         means that if a value was read once within transaction, then all consecutive reads will provide 
    ///         the same in-transaction value. With this isolation level accessed values are stored within 
    ///         in-transaction memory, so consecutive access to the same key within the same transaction will always 
    ///         return the value that was previously read or updated within this transaction. If concurrency is 
    ///         <see cref="TransactionConcurrency.Pessimistic"/>, then a lock on the key will be 
    ///         acquired prior to accessing the value.</description>
    ///     </item>
    ///     <item>
    ///         <description><see cref="TransactionIsolation.Serializable"/> isolation level means 
    ///         that all transactions occur in a completely isolated fashion, as if all transactions in the system 
    ///         had executed serially, one after the other. Read access with this level happens the same way as with 
    ///         <see cref="TransactionIsolation.RepeatableRead"/> level. However, in 
    ///         <see cref="TransactionConcurrency.Optimistic"/> mode, if some transactions cannot be 
    ///         serially isolated from each other, then one winner will be picked and the other transactions in 
    ///         conflict will result in <c>TransactionOptimisticException</c> being thrown on Java side.</description>
    ///     </item>
    /// </list>
    /// Cache transactions support the following concurrency models:
    /// <list type="bullet">
    ///     <item>
    ///         <description><see cref="TransactionConcurrency.Optimistic"/> - in this mode all cache 
    ///         operations 
    ///         are not distributed to other nodes until <see cref="ITransaction.Commit()"/>.
    ///         In this mode one <c>PREPARE</c> message will 
    ///         be sent to participating cache nodes to start acquiring per-transaction locks, and once all nodes 
    ///         reply <c>OK</c> (i.e. <c>Phase 1</c> completes successfully), a one-way <c>COMMIT</c> message is sent
    ///         without waiting for reply. If it is necessary to know whenever remote nodes have committed as well, 
    ///         synchronous commit or synchronous rollback should be enabled via 
    ///         <c>CacheConfiguration.setWriteSynchronizationMode</c>.
    ///         <para />
    ///         Note that in this mode, optimistic failures are only possible in conjunction with
    ///         <see cref="TransactionIsolation.Serializable"/> isolation level. In all other cases, 
    ///         optimistic transactions will never fail optimistically and will always be identically ordered on all 
    ///         participating Ignite nodes.</description>
    ///     </item>
    ///     <item>
    ///         <description><see cref="TransactionConcurrency.Pessimistic"/> - in this mode a lock is 
    ///         acquired on all cache operations with exception of read operations in 
    ///         <see cref="TransactionIsolation.ReadCommitted"/> mode. All optional filters passed 
    ///         into cache operations will be evaluated after successful lock acquisition. Whenever 
    ///         <see cref="ITransaction.Commit()"/> is called, a single one-way <c>COMMIT</c> 
    ///         message is sent to participating cache nodes without waiting for reply. Note that there is no reason 
    ///         for distributed <c>PREPARE</c> step, as all locks have been already acquired. Just like with 
    ///         optimistic mode, it is possible to configure synchronous commit or rollback and wait till 
    ///         transaction commits on all participating remote nodes.</description>
    ///     </item>
    /// </list>
    /// <para />
    /// In addition to standard <c>CacheAtomicityMode.TRANSACTIONAL</c> behavior, Ignite also supports
    /// a lighter <c>CacheAtomicityMode.ATOMIC</c> mode as well. In this mode distributed transactions
    /// and distributed locking are not supported. Disabling transactions and locking allows to achieve much higher
    /// performance and throughput ratios. It is recommended that <c>CacheAtomicityMode.TRANSACTIONAL</c> mode
    /// is used whenever full <c>ACID</c>-compliant transactions are not needed.
    /// <example>
    ///     You can use cache transactions as follows:
    ///     <code>
    ///     ICacheTx tx = cache.TxStart();    
    /// 
    ///     try 
    ///     {
    ///         int v1 = cache&lt;string, int&gt;.Get("k1");
    ///         
    ///         // Check if v1 satisfies some condition before doing a put.
    ///         if (v1 > 0)
    ///             cache.Put&lt;string, int&gt;("k1", 2);
    ///             
    ///         cache.Removex("k2);
    ///         
    ///         // Commit the transaction.
    ///         tx.Commit();
    ///     }
    ///     finally 
    ///     {
    ///         tx.Dispose();
    ///     }
    ///     
    ///     </code>
    /// </example>
    /// </summary>
    public interface ITransaction : IDisposable, IAsyncSupport<ITransaction>
    {
        /// <summary>
        /// ID of the node on which this transaction started.
        /// </summary>
        /// <value>
        /// Originating node ID.
        /// </value>
        Guid NodeId { get; }

        /// <summary>
        /// ID of the thread in which this transaction started.
        /// </summary>
        long ThreadId
        {
            get;
        }

        /// <summary>
        /// Start time of this transaction on this node.
        /// </summary>
        DateTime StartTime
        {
            get;
        }

        /// <summary>
        /// Transaction isolation level.
        /// </summary>
        TransactionIsolation Isolation
        {
            get;
        }

        /// <summary>
        /// Transaction concurrency mode.
        /// </summary>
        TransactionConcurrency Concurrency
        {
            get;
        }

        /// <summary>
        /// Current transaction state.
        /// </summary>
        TransactionState State
        {
            get;
        }

        /// <summary>
        /// Timeout value in milliseconds for this transaction. If transaction times
        /// out prior to it's completion, an exception will be thrown.
        /// </summary>
        TimeSpan Timeout
        {
            get;
        }

        /// <summary>
        /// Gets a value indicating whether this transaction was marked as rollback-only.
        /// </summary>
        bool IsRollbackOnly
        {
            get;
        }

        /// <summary>
        /// Modify the transaction associated with the current thread such that the 
        /// only possible outcome of the transaction is to roll back the transaction.
        /// </summary>
        /// <returns>
        /// True if rollback-only flag was set as a result of this operation, 
        /// false if it was already set prior to this call or could not be set
        /// because transaction is already finishing up committing or rolling back.
        /// </returns>
        bool SetRollbackonly();

        /// <summary>
        /// Commits this transaction.
        /// </summary>
        [AsyncSupported]
        void Commit();

        /// <summary>
        /// Rolls back this transaction.
        /// </summary>
        [AsyncSupported]
        void Rollback();

        /// <summary>
        /// Adds a new metadata.
        /// </summary>
        /// <param name="name">Metadata name.</param>
        /// <param name="val">Metadata value.</param>
        void AddMeta<TV>(string name, TV val);

        /// <summary>
        /// Gets metadata by name.
        /// </summary>
        /// <param name="name">Metadata name.</param>
        /// <returns>Metadata value.</returns>
        /// <exception cref="KeyNotFoundException">If metadata key was not found.</exception>
        TV Meta<TV>(string name);

        /// <summary>
        /// Removes metadata by name.
        /// </summary>
        /// <param name="name">Metadata name.</param>
        /// <returns>Value of removed metadata or default value for <code>V</code> type.</returns>
        TV RemoveMeta<TV>(string name);
    }
}
