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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Base event implementation.
    /// </summary>
    public abstract class EventBase : IEvent, IEquatable<EventBase>
    {
        /** */
        private readonly GridGuid id;

        /** */
        private readonly long localOrder;

        /** */
        private readonly IClusterNode node;

        /** */
        private readonly string message;

        /** */
        private readonly int type;

        /** */
        private readonly string name;

        /** */
        private readonly DateTime timeStamp;

        /// <summary>
        /// Initializes a new instance of the <see cref="EventBase"/> class.
        /// </summary>
        /// <param name="r">The reader to read data from.</param>
        protected EventBase(IPortableRawReader r)
        {
            id = GridGuid.ReadPortable(r);

            localOrder = r.ReadLong();

            node = ReadNode(r);

            message = r.ReadString();
            type = r.ReadInt();
            name = r.ReadString();
            timeStamp = r.ReadDate() ?? DateTime.Now;
        }

        /** <inheritDoc /> */
        public GridGuid Id
        {
            get { return id; }
        }

        /** <inheritDoc /> */
        public long LocalOrder
        {
            get { return localOrder; }
        }

        /** <inheritDoc /> */
        public IClusterNode Node
        {
            get { return node; }
        }

        /** <inheritDoc /> */
        public string Message
        {
            get { return message; }
        }

        /** <inheritDoc /> */
        public int Type
        {
            get { return type; }
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return name; }
        }

        /** <inheritDoc /> */
        public DateTime TimeStamp
        {
            get { return timeStamp; }
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
            
            return id.Equals(other.id);
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
            return id.GetHashCode();
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return string.Format("CacheEntry [Name={0}, Type={1}, TimeStamp={2}, Message={3}]", Name, Type, TimeStamp,
                Message);
        }

        /// <summary>
        /// Reads a node from stream.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Node or null.</returns>
        protected static IClusterNode ReadNode(IPortableRawReader reader)
        {
            return ((PortableReaderImpl)reader).Marshaller.Grid.GetNode(reader.ReadGuid());
        }
    }
}