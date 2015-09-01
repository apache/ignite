/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Events
{
    using System;

    /// <summary>
    /// Represents an event filter.
    /// </summary>
    /// <typeparam name="T">Event type.</typeparam>
    public interface IEventFilter<in T> where T : IEvent
    {
        /// <summary>
        /// Determines whether specified event passes this filtger.
        /// </summary>
        /// <param name="nodeId">Node identifier.</param>
        /// <param name="evt">Event.</param>
        /// <returns>Value indicating whether specified event passes this filtger.</returns>
        bool Invoke(Guid nodeId, T evt);
    }
}