/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Events
{
    using System;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Represents a Ignite event.
    /// </summary>
    public interface IEvent
    {
        /// <summary>
        /// Gets globally unique ID of this event.
        /// </summary>
        IgniteGuid Id { get; }

        /// <summary>
        /// Gets locally unique ID that is atomically incremented for each event. Unlike global <see cref="Id" />
        /// this local ID can be used for ordering events on this node. 
        /// <para/> 
        /// Note that for performance considerations Ignite doesn't order events globally.
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
        DateTime Timestamp { get; }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
        string ToShortString();
    }
}