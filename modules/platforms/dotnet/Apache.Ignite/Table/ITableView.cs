/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Table
{
    using System.Threading.Tasks;

    /// <summary>
    /// Table view interface provides methods to access table records.
    /// <para />
    /// TODO: All APIs (IGNITE-15430).
    /// </summary>
    /// <typeparam name="T">Record type.</typeparam>
    public interface ITableView<T>
        where T : class
    {
        /// <summary>
        /// Gets a record by key.
        /// </summary>
        /// <param name="keyRec">A record with key columns set.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task<T?> GetAsync(T keyRec);

        /// <summary>
        /// Inserts a record into the table if it does not exist or replaces the existing one.
        /// </summary>
        /// <param name="rec">Record to upsert.</param>
        /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
        Task UpsertAsync(T rec);
    }
}
