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

namespace Apache.Ignite.Core.Impl.Events
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Event filter/listener holder for RemoteListen.
    /// </summary>
    internal class RemoteListenEventFilter : IInteropCallback
    {
        /** */
        private readonly GridImpl grid;
        
        /** */
        private readonly Func<Guid, IEvent, bool> filter;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteListenEventFilter"/> class.
        /// </summary>
        /// <param name="grid">The grid.</param>
        /// <param name="filter">The filter.</param>
        public RemoteListenEventFilter(GridImpl grid, Func<Guid, IEvent, bool> filter)
        {
            this.grid = grid;
            this.filter = filter;
        }

        /** <inheritdoc /> */
        public int Invoke(IPortableStream stream)
        {
            var reader = grid.Marshaller.StartUnmarshal(stream);

            var evt = EventReader.Read<IEvent>(reader);

            var nodeId = reader.ReadGuid() ?? Guid.Empty;

            return filter(nodeId, evt) ? 1 : 0;
        }

        /// <summary>
        /// Creates an instance of this class from a stream.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="grid">Grid</param>
        /// <returns>Deserialized instance of <see cref="RemoteListenEventFilter"/></returns>
        public static RemoteListenEventFilter CreateInstance(long memPtr, GridImpl grid)
        {
            Debug.Assert(grid != null);

            using (var stream = GridManager.Memory.Get(memPtr).Stream())
            {
                var marsh = grid.Marshaller;

                var reader = marsh.StartUnmarshal(stream);

                var pred = reader.ReadObject<PortableOrSerializableObjectHolder>().Item;

                var func = DelegateTypeDescriptor.GetEventFilter(pred.GetType());

                return new RemoteListenEventFilter(grid, (id, evt) => func(pred, id, evt));
            }
        }
    }
}