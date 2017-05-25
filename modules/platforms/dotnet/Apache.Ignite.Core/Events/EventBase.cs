/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Events
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Base event implementation.
    /// </summary>
    public abstract class EventBase : IEvent, IEquatable<EventBase>
    {
        /** */
        private readonly IgniteGuid _id;

        /** */
        private readonly long _localOrder;

        /** */
        private readonly IClusterNode _node;

        /** */
        private readonly string _message;

        /** */
        private readonly int _type;

        /** */
        private readonly string _name;

        /** */
        private readonly DateTime _timestamp;

        /// <summary>
        /// Initializes a new instance of the <see cref="EventBase"/> class.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected EventBase(IBinaryRawReader r)
        {
            Debug.Assert(r != null);

            _id = r.ReadObject<IgniteGuid>();

            _localOrder = r.ReadLong();

            _node = ReadNode(r);

            _message = r.ReadString();
            _type = r.ReadInt();
            _name = r.ReadString();
            
            var timestamp = r.ReadTimestamp();
            Debug.Assert(timestamp.HasValue);
            _timestamp = timestamp.Value;
        }

        /// <summary>
        /// Gets globally unique ID of this event.
        /// </summary>
        public IgniteGuid Id
        {
            get { return _id; }
        }

        /// <summary>
        /// Gets locally unique ID that is atomically incremented for each event. Unlike global <see cref="Id" />
        /// this local ID can be used for ordering events on this node.
        /// <para />
        /// Note that for performance considerations Ignite doesn't order events globally.
        /// </summary>
        public long LocalOrder
        {
            get { return _localOrder; }
        }

        /// <summary>
        /// Node where event occurred and was recorded.
        /// </summary>
        public IClusterNode Node
        {
            get { return _node; }
        }

        /// <summary>
        /// Gets optional message for this event.
        /// </summary>
        public string Message
        {
            get { return _message; }
        }

        /// <summary>
        /// Gets type of this event. All system event types are defined in <see cref="EventType" />
        /// </summary>
        public int Type
        {
            get { return _type; }
        }

        /// <summary>
        /// Gets name of this event.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Gets event timestamp. Timestamp is local to the node on which this event was produced.
        /// Note that more than one event can be generated with the same timestamp.
        /// For ordering purposes use <see cref="LocalOrder" /> instead.
        /// </summary>
        public DateTime Timestamp
        {
            get { return _timestamp; }
        }

        /// <summary>
        /// Gets shortened version of ToString result.
        /// </summary>
        public virtual string ToShortString()
        {
            return ToString();
        }

        /// <summary>
        /// Determines whether the specified object is equal to this instance.
        /// </summary>
        /// <param name="other">The object to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified object is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public bool Equals(EventBase other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            
            return _id.Equals(other._id);
        }

        /// <summary>
        /// Determines whether the specified object is equal to this instance.
        /// </summary>
        /// <param name="obj">The object to compare with this instance.</param>
        /// <returns>
        ///   <c>true</c> if the specified object is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            
            return Equals((EventBase) obj);
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public override int GetHashCode()
        {
            return _id.GetHashCode();
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, 
                "{0} [Name={1}, Type={2}, Timestamp={3}, Message={4}]", GetType().Name, Name, Type, Timestamp, Message);
        }

        /// <summary>
        /// Reads a node from stream.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Node or null.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected static IClusterNode ReadNode(IBinaryRawReader reader)
        {
            return ((BinaryReader)reader).Marshaller.Ignite.GetNode(reader.ReadGuid());
        }
    }
}