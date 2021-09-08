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

namespace Apache.Ignite.Core.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Attribute node filter.
    /// <para />
    /// The filter will evaluate to true if a node has all specified attributes with corresponding values.
    /// <para />
    /// You can set node attributes using <see cref="IgniteConfiguration.UserAttributes"/> property. 
    /// </summary>
    public sealed class AttributeNodeFilter : IClusterNodeFilter
    {
        /** */
        private IDictionary<string, object> _attributes;

        /// <summary>
        /// Attributes dictionary match.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public IDictionary<string, object> Attributes
        {
            get { return _attributes; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException("value");
                }

                _attributes = value;
            }
        }

        /// <summary>
        /// Initializes a new instance of <see cref="AttributeNodeFilter"/>.
        /// </summary>
        public AttributeNodeFilter()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="AttributeNodeFilter"/>.
        /// </summary>
        /// <param name="attrName">Attribute name.</param>
        /// <param name="attrValue">Attribute value.</param>
        public AttributeNodeFilter(string attrName, object attrValue)
        {
            IgniteArgumentCheck.NotNullOrEmpty(attrName, "attrName");

            Attributes = new Dictionary<string, object>(1)
            {
                {attrName, attrValue}
            };
        }

        /** <inheritdoc /> */
        public bool Invoke(IClusterNode node)
        {
            throw new NotSupportedException("Should not be called from .NET side.");
        }

        /// <summary>
        /// Initializes a new instance of <see cref="AttributeNodeFilter"/> from a binary reader.
        /// </summary>
        /// <param name="reader">Reader.</param>
        internal AttributeNodeFilter(IBinaryRawReader reader)
        {
            IgniteArgumentCheck.NotNull(reader, "reader");

            int count = reader.ReadInt();

            Debug.Assert(count > 0);

            Attributes = new Dictionary<string, object>(count);

            while (count > 0)
            {
                string attrKey = reader.ReadString();
                object attrVal = reader.ReadObject<object>();

                Debug.Assert(attrKey != null);

                Attributes[attrKey] = attrVal;

                count--;
            }
        }

        /// <summary>
        /// Writes the instance to a writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteInt(Attributes.Count);

            // Does not preserve ordering, it's fine.
            foreach (KeyValuePair<string, object> attr in Attributes)
            {
                writer.WriteString(attr.Key);
                writer.WriteObject(attr.Value);
            }
        }
    }
}
