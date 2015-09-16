/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Threading;
using GridGain.Events;

namespace GridGain.Examples.Events
{
    /// <summary>
    /// Local event listener.
    /// </summary>
    public class LocalListener : IEventFilter<IEvent>
    {
        /** Сount of received events. */
        private int _eventsReceived;

        /// <summary>
        /// Gets the count of received events.
        /// </summary>
        public int EventsReceived
        {
            get { return _eventsReceived; }
        }

        /// <summary>
        /// Determines whether specified event passes this filter.
        /// </summary>
        /// <param name="nodeId">Node identifier.</param>
        /// <param name="evt">Event.</param>
        /// <returns>Value indicating whether specified event passes this filter.</returns>
        public bool Invoke(Guid nodeId, IEvent evt)
        {
            Interlocked.Increment(ref _eventsReceived);

            Console.WriteLine("Local listener received an event [evt={0}]", evt.Name);

            return true;
        }
    }
}
