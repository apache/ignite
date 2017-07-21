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
