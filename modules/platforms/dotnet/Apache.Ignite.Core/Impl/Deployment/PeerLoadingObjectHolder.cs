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
