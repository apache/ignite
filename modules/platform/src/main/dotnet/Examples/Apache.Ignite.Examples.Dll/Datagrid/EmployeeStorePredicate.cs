/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using GridGain.Cache;
using GridGain.Examples.Portable;

namespace GridGain.Examples.Datagrid
{
    /// <summary>
    /// Example cache entry predicate.
    /// </summary>
    [Serializable]
    public class EmployeeStorePredicate : ICacheEntryFilter<int, Employee>
    {
        /// <summary>
        /// Returns a value indicating whether provided cache entry satisfies this predicate.
        /// </summary>
        /// <param name="entry">Cache entry.</param>
        /// <returns>Value indicating whether provided cache entry satisfies this predicate.</returns>
        public bool Invoke(ICacheEntry<int, Employee> entry)
        {
            return entry.Key == 1;
        }
    }
}