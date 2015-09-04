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

namespace Apache.Ignite.Core.Cache.Query.Continuous
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Represents a continuous query handle.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1040:AvoidEmptyInterfaces")]
    public interface IContinuousQueryHandle : IDisposable
    {
        // No-op.
    }

    /// <summary>
    /// Represents a continuous query handle.
    /// </summary>
    /// <typeparam name="T">Type of the initial query cursor.</typeparam>
    public interface IContinuousQueryHandle<T> : IContinuousQueryHandle
    {
        /// <summary>
        /// Gets the cursor for initial query.
        /// </summary>
        [Obsolete("GetInitialQueryCursor() method should be used instead.")]
        IQueryCursor<T> InitialQueryCursor { get; }

        /// <summary>
        /// Gets the cursor for initial query.
        /// Can be called only once, throws exception on consequent calls.
        /// </summary>
        /// <returns>Initial query cursor.</returns>
        IQueryCursor<T> GetInitialQueryCursor();
    }
}