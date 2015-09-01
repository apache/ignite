/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using GridGain.Cache.Query;

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