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

namespace Apache.Ignite.Core.Cache.Query
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Query result cursor. Can be processed either in iterative mode, or by taking
    /// all entries using <see cref="IQueryCursor{T}.GetAll()"/> method.
    /// <para />
    /// Note that you get enumerator or call <code>GetAll()</code> method only once during
    /// cursor lifetime. Any further attempts to get enumerator or all entries will result 
    /// in exception.
    /// </summary>
    public interface IQueryCursor<T> : IEnumerable<T>, IDisposable
    {
        /// <summary>
        /// Gets all query results. Use this method when you know in advance that query 
        /// result is relatively small and will not cause memory utilization issues.
        /// </summary>
        /// <returns>List containing all query results.</returns>
        IList<T> GetAll();
    }
}
