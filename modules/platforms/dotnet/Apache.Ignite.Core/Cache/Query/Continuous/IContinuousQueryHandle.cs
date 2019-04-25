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
        /// Can be called only once, throws exception on consequent calls.
        /// </summary>
        /// <returns>Initial query cursor.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate",
            Justification = "Semantics: result differs from call to call.")]
        IQueryCursor<T> GetInitialQueryCursor();
    }
}