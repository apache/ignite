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

namespace Apache.Ignite.ExamplesDll.Binary
{
    using System;

    /// <summary>
    /// Account object. Used in transaction example.
    /// </summary>
    public class Account
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">Account ID.</param>
        /// <param name="balance">Account balance.</param>
        public Account(int id, decimal balance)
        {
            Id = id;
            Balance = balance;
        }

        /// <summary>
        /// Account ID.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Account balance.
        /// </summary>
        public decimal Balance { get; set; }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override String ToString()
        {
            return string.Format("{0} [id={1}, balance={2}]", typeof(Account).Name, Id, Balance);
        }
    }
}
