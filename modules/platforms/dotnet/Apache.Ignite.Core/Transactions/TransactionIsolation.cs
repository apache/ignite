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
    /// Defines different cache transaction isolation levels. See <see cref="ITransaction"/>
    /// documentation for more information about cache transaction isolation levels.
    /// </summary>
    public enum TransactionIsolation
    {
        /// <summary>
        /// Read committed isolation level.
        /// </summary>
        ReadCommitted = 0,

        /// <summary>
        /// Repeatable read isolation level.
        /// </summary>
        RepeatableRead = 1,

        /// <summary>
        /// Serializable isolation level.
        /// </summary>
        Serializable = 2
    }
}
