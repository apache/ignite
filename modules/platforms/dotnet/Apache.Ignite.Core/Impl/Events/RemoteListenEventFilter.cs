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

namespace Apache.Ignite.Core.Impl.Events
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Event filter/listener holder for RemoteListen.
    /// </summary>
    internal class RemoteListenEventFilter : IInteropCallback
    {
        /** */
        private readonly Ignite _ignite;
        
        /** */
        private readonly Func<IEvent, bool> _filter;

        /// <summary>
        /// Initializes a new instance of the <see cref="RemoteListenEventFilter"/> class.
        /// </summary>
        /// <param name="ignite">The grid.</param>
        /// <param name="filter">The filter.</param>
        public RemoteListenEventFilter(Ignite ignite, Func<IEvent, bool> filter)
        {
            _ignite = ignite;
            _filter = filter;
        }

        /** <inheritdoc /> */
        public int Invoke(IBinaryStream stream)
        {
            var reader = _ignite.Marshaller.StartUnmarshal(stream);

            var evt = EventReader.Read<IEvent>(reader);

            reader.ReadGuid();  // unused node id

            return _filter(evt) ? 1 : 0;
        }

        /// <summary>
        /// Creates an instance of this class from a stream.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="grid">Grid</param>
        /// <returns>Deserialized instance of <see cref="RemoteListenEventFilter"/></returns>
        public static RemoteListenEventFilter CreateInstance(long memPtr, Ignite grid)
        {
            Debug.Assert(grid != null);

            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var marsh = grid.Marshaller;

                var reader = marsh.StartUnmarshal(stream);

                var pred = reader.ReadObject<object>();

                ResourceProcessor.Inject(pred, grid);

                var func = DelegateTypeDescriptor.GetEventFilter(pred.GetType());

                return new RemoteListenEventFilter(grid, evt => func(pred, evt));
            }
        }
    }
}