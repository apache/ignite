/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using GridGain.Events;

namespace GridGain.Examples.Events
{
    /// <summary>
    /// Remote event filter.
    /// </summary>
    [Serializable]
    public class RemoteFilter : IEventFilter<IEvent>
    {
        /// <summary>
        /// Determines whether specified event passes this filter.
        /// </summary>
        /// <param name="nodeId">Node identifier.</param>
        /// <param name="evt">Event.</param>
        /// <returns>Value indicating whether specified event passes this filter.</returns>
        public bool Invoke(Guid nodeId, IEvent evt)
        {
            Console.WriteLine("Remote filter received event [evt={0}]", evt.Name);

            return evt is JobEvent;
        }
    }
}