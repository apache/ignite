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

namespace Apache.Ignite.Core.Impl.Deployment
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Holds an object which can have it's assembly automatically loaded on remote nodes.
    /// 
    /// Contains assembly-qualified type name.
    /// Types from assemblies with different versions can coexist and will be differentiated properly.
    /// </summary>
    internal class PeerLoadingObjectHolder : IBinaryWriteAware
    {
        /** Object. */
        private readonly object _object;

        /// <summary>
        /// Initializes a new instance of the <see cref="PeerLoadingObjectHolder"/> class.
        /// </summary>
        /// <param name="o">The object.</param>
        public PeerLoadingObjectHolder(object o)
        {
            Debug.Assert(o != null);

            _object = o;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PeerLoadingObjectHolder"/> class.
        /// </summary>
        public PeerLoadingObjectHolder(BinaryReader reader)
        {
            Debug.Assert(reader != null);

            var originNodeId = reader.ReadGuid().GetValueOrDefault();
            
            var typeName = reader.ReadString();

            var ignite = reader.Marshaller.Ignite;

            using (new PeerAssemblyResolver(ignite, originNodeId))  // Resolve transitive dependencies when needed.
            {
                // Resolve type from existing assemblies or from remote nodes.
                var type = Type.GetType(typeName, false)
                           ?? PeerAssemblyResolver.LoadAssemblyAndGetType(typeName, ignite, originNodeId);

                Debug.Assert(type != null);

                _object = reader.Deserialize<object>(type);
            }
        }

        /// <summary>
        /// Gets the object.
        /// </summary>
        public object Object
        {
            get { return _object; }
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter) writer.GetRawWriter();

            writer0.WriteGuid(writer0.Marshaller.Ignite.GetIgnite().GetCluster().GetLocalNode().Id);
            writer0.WriteString(_object.GetType().AssemblyQualifiedName);
            writer0.WriteObjectDetached(_object);
        }
    }
}
