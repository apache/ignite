/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Query
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
