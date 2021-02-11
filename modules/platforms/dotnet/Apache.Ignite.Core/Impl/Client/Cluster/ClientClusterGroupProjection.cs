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

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Projection builder that is used for remote nodes filtering.
    /// </summary>
    internal sealed class ClientClusterGroupProjection
    {
        /** */
        private const int Attribute = 1;

        /** */
        private const int ServerNodes = 2;

        /** Filter value mappings. */
        private readonly List<IProjectionItem> _filter;

        /// <summary>
        /// Empty constructor.
        /// </summary>
        private ClientClusterGroupProjection()
        {
            _filter = new List<IProjectionItem>();
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proj">Source projection.</param>
        /// <param name="item">New item.</param>
        private ClientClusterGroupProjection(ClientClusterGroupProjection proj, IProjectionItem item)
        {
            _filter = new List<IProjectionItem>(proj._filter) {item};
        }

        /// <summary>
        /// Creates a new projection instance with specified attribute.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="value">Attribute value.</param>
        /// <returns>Projection instance.</returns>
        public ClientClusterGroupProjection ForAttribute(string name, string value)
        {
            return new ClientClusterGroupProjection(this, new ForAttributeProjectionItem(name, value));
        }

        /// <summary>
        /// Creates a new projection with server nodes only.
        /// </summary>
        /// <returns>Projection instance.</returns>
        public ClientClusterGroupProjection ForServerNodes(bool value)
        {
            return new ClientClusterGroupProjection(this, new ForServerNodesProjectionItem(value));
        }

        /// <summary>
        /// Initializes an empty projection instance.
        /// </summary>
        public static ClientClusterGroupProjection Empty
        {
            get { return new ClientClusterGroupProjection(); }
        }

        /// <summary>
        /// Writes the projection to output buffer.
        /// </summary>
        /// <param name="writer">Binary writer.</param>
        public void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt(_filter.Count);

            foreach (var item in _filter)
            {
                item.Write(writer);
            }
        }

        /// <summary>
        /// Projection item.
        /// </summary>
        private interface IProjectionItem
        {
            /// <summary>
            /// Writes the projection item to output buffer.
            /// </summary>
            /// <param name="writer">Binary writer.</param>
            void Write(IBinaryRawWriter writer);
        }

        /// <summary>
        /// Represents attribute projection item.
        /// </summary>
        private sealed class ForAttributeProjectionItem : IProjectionItem
        {
            /** */
            private readonly string _key;

            /** */
            private readonly string _value;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="key">Attribute key.</param>
            /// <param name="value">Attribute value.</param>
            public ForAttributeProjectionItem(string key, string value)
            {
                _key = key;
                _value = value;
            }

            /** <inheritDoc /> */
            public void Write(IBinaryRawWriter writer)
            {
                writer.WriteShort(Attribute);
                writer.WriteString(_key);
                writer.WriteString(_value);
            }
        }

        /// <summary>
        /// Represents server nodes only projection item.
        /// </summary>
        private sealed class ForServerNodesProjectionItem : IProjectionItem
        {
            /** */
            private readonly bool _value;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="value"><c>True</c> for server nodes only.
            /// <c>False</c> for client nodes only.</param>
            public ForServerNodesProjectionItem(bool value)
            {
                _value = value;
            }

            /** <inheritDoc /> */
            public void Write(IBinaryRawWriter writer)
            {
                writer.WriteShort(ServerNodes);
                writer.WriteBoolean(_value);
            }
        }
    }
}
