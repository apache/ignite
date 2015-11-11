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
        protected EventBase(IBinaryRawReader r)
        {
            var id = IgniteGuid.Read(r);
            Debug.Assert(id.HasValue);
            _id = id.Value;

            _localOrder = r.ReadLong();

            _node = ReadNode(r);

            _message = r.ReadString();
            _type = r.ReadInt();
            _name = r.ReadString();
            
            var timestamp = r.ReadTimestamp();
            Debug.Assert(timestamp.HasValue);
            _timestamp = timestamp.Value;
        }

        /** <inheritDoc /> */
        public IgniteGuid Id
        {
            get { return _id; }
        }

        /** <inheritDoc /> */
        public long LocalOrder
        {
            get { return _localOrder; }
        }

        /** <inheritDoc /> */
        public IClusterNode Node
        {
            get { return _node; }
        }

        /** <inheritDoc /> */
        public string Message
        {
            get { return _message; }
        }

        /** <inheritDoc /> */
        public int Type
        {
            get { return _type; }
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public DateTime Timestamp
        {
            get { return _timestamp; }
        }

        /** <inheritDoc /> */
        public virtual string ToShortString()
        {
            return ToString();
        }

        /** <inheritDoc /> */
        public bool Equals(EventBase other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            
            return _id.Equals(other._id);
        }

        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            
            return Equals((EventBase) obj);
        }

        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            return _id.GetHashCode();
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, 
                "CacheEntry [Name={0}, Type={1}, Timestamp={2}, Message={3}]", Name, Type, Timestamp, Message);
        }

        /// <summary>
        /// Reads a node from stream.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Node or null.</returns>
        protected static IClusterNode ReadNode(IBinaryRawReader reader)
        {
            return ((BinaryReader)reader).Marshaller.Ignite.GetNode(reader.ReadGuid());
        }
    }
}