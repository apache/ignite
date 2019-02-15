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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading;

    /// <summary>
    /// Tracks object graph and invokes <see cref="IDeserializationCallback" />.
    /// <para />
    /// <see cref="IDeserializationCallback.OnDeserialization" /> must be called after entire object graph has been
    /// deserialized. We preserve all objects in a thread-local list and invoke callbacks once all objects
    /// are fully deserialized.
    /// </summary>
    internal static class DeserializationCallbackProcessor
    {
        /// <summary>
        /// Object graph for current thread.
        /// </summary>
        private static readonly ThreadLocal<ObjectGraph> Graph 
            = new ThreadLocal<ObjectGraph>(() => new ObjectGraph());

        /// <summary>
        /// Register an object for deserialization callback.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>Id of the object.</returns>
        public static void Push(object obj)
        {
            var graph = Graph.Value;

            graph.Depth++;

            var cb = obj as IDeserializationCallback;

            if (cb != null)
            {
                graph.Objects.Add(cb);
            }
        }

        /// <summary>
        /// Called when deserialization of an object has completed.
        /// When Pop() has been called for all registered objects, all callbacks are invoked.
        /// </summary>
        public static void Pop()
        {
            var graph = Graph.Value;

            graph.Depth--;

            if (graph.Depth == 0)
            {
                // Entire graph has been deserialized: invoke callbacks in direct order (like BinaryFormatter does).
                foreach (var obj in graph.Objects)
                {
                    obj.OnDeserialization(null);
                }

                graph.Objects.Clear();
            }
        }

        /// <summary>
        /// Clears all registered objects.
        /// </summary>
        public static void Clear()
        {
            var graph = Graph.Value;
            
            graph.Objects.Clear();
            graph.Depth = 0;
        }

        /// <summary>
        /// Object graph.
        /// </summary>
        private class ObjectGraph
        {
            /** */
            private readonly List<IDeserializationCallback> _objects = new List<IDeserializationCallback>();

            /// <summary>
            /// Gets or sets the depth.
            /// </summary>
            public int Depth { get; set; }

            /// <summary>
            /// Gets the objects.
            /// </summary>
            public List<IDeserializationCallback> Objects
            {
                get { return _objects; }
            }
        }
    }
}
