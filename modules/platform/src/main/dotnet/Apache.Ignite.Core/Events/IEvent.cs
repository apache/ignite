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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Represents a grid event.
    /// </summary>
    public interface IEvent
    {
        /// <summary>
        /// Gets globally unique ID of this event.
        /// </summary>
        GridGuid Id { get; }

        /// <summary>
        /// Gets locally unique ID that is atomically incremented for each event. Unlike global <see cref="Id" />
        /// this local ID can be used for ordering events on this node. 
        /// <para/> 
        /// Note that for performance considerations GridGain doesn't order events globally.
        /// </summary>
        long LocalOrder { get; }

        /// <summary>
        /// Node where event occurred and was recorded.
        /// </summary>
        IClusterNode Node { get; }

        /// <summary>
        /// Gets optional message for this event.
        /// </summary>
        string Message { get; }

        /// <summary>
        /// Gets type of this event. All system event types are defined in <see cref="EventType"/>
        /// </summary>
        int Type { get; }

        /// <summary>
        /// Gets name of this event.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets event timestamp. Timestamp is local to the node on which this event was produced. 
        /// Note that more than one event can be generated with the same timestamp. 
        /// For ordering purposes use <see cref="LocalOrder"/> instead.
        /// </summary>
        DateTime TimeStamp { get; }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
        string ToShortString();
    }
}