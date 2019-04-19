/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Transactions
{
    /// <summary>
    /// Cache transaction state.
    /// </summary>
    public enum TransactionState
    {
        /// <summary>
        /// Transaction started.
        /// </summary>
        Active,

        /// <summary>
        /// Transaction validating.
        /// </summary>
        Preparing,

        /// <summary>
        /// Transaction validation succeeded.
        /// </summary>
        Prepared,

        /// <summary>
        /// Transaction is marked for rollback.
        /// </summary>
        MarkedRollback,

        /// <summary>
        /// Transaction commit started (validating finished).
        /// </summary>
        Committing,

        /// <summary>
        /// Transaction commit succeeded.
        /// </summary>
        Committed,
        
        /// <summary>
        /// Transaction rollback started (validation failed).
        /// </summary>
        RollingBack,

        /// <summary>
        /// Transaction rollback succeeded.
        /// </summary>
        RolledBack,

        /// <summary>
        /// Transaction rollback failed or is otherwise unknown state.
        /// </summary>
        Unknown
    }
}
