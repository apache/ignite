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

namespace Apache.Ignite.Core.Impl.Cluster
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Baseline node.
    /// </summary>
    internal class BaselineNode : IBaselineNode
    {
        /** Attributes. */
        private readonly IDictionary<string, object> _attributes;

        /** Consistent ID. */
        private readonly object _consistentId;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaselineNode"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public BaselineNode(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            _consistentId = reader.ReadObject<object>();
            _attributes = ClusterNodeImpl.ReadAttributes(reader);
        }

        /** <inheritdoc /> */
        public object ConsistentId
        {
            get { return _consistentId; }
        }

        /** <inheritdoc /> */
        public IDictionary<string, object> Attributes
        {
            get { return _attributes; }
        }

        /// <summary>
        /// Writes this instance to specified writer.
        /// </summary>
        public static void Write(BinaryWriter writer, IBaselineNode node)
        {
            Debug.Assert(writer != null);
            Debug.Assert(node != null);

            writer.WriteObjectDetached(node.ConsistentId);
            
            var attrs = node.Attributes;

            if (attrs != null)
            {
                writer.WriteInt(attrs.Count);

                foreach (var attr in attrs)
                {
                    writer.WriteString(attr.Key);
                    writer.WriteObjectDetached(attr.Value);
                }
            }
            else
            {
                writer.WriteInt(0);
            }
        }
    }
}
