/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using GridGain.Cache.Event;

namespace GridGain.Examples.Datagrid
{
    /// <summary>
    /// Filter for continuous query example.
    /// </summary>
    [Serializable]
    public class ContinuousQueryFilter : ICacheEntryEventFilter<int, string>
    {
        /// <summary> Threshold. </summary>
        private readonly int _threshold;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="threshold">Threshold.</param>
        public ContinuousQueryFilter(int threshold)
        {
            _threshold = threshold;
        }

        /// <summary>
        /// Evaluates cache entry event.
        /// </summary>
        /// <param name="evt">Event.</param>
        public bool Evaluate(ICacheEntryEvent<int, string> evt)
        {
            return evt.Key >= _threshold;
        }
    }
}
