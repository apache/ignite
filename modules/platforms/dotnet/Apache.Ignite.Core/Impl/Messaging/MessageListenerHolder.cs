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

namespace Apache.Ignite.Core.Impl.Messaging
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Messaging;

    /// <summary>
    /// Non-generic binary message listener wrapper.
    /// </summary>
    internal class MessageListenerHolder : IBinaryWriteAware, IHandle
    {
        /** Invoker function that takes key and value and invokes wrapped IMessageListener */
        private readonly Func<Guid, object, bool> _invoker;

        /** Current Ignite instance. */
        private readonly IIgniteInternal _ignite;
        
        /** Underlying filter. */
        private readonly object _filter;

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageListenerHolder" /> class.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="filter">The <see cref="IMessageListener{T}" /> to wrap.</param>
        /// <param name="invoker">The invoker func that takes key and value and invokes wrapped IMessageListener.</param>
        private MessageListenerHolder(Ignite grid, object filter, Func<Guid, object, bool> invoker)
        {
            Debug.Assert(filter != null);
            Debug.Assert(invoker != null);

            _invoker = invoker;

            _filter = filter;

            // 1. Set fields.
            Debug.Assert(grid != null);

            _ignite = grid;
            _invoker = invoker;

            // 2. Perform injections.
            ResourceProcessor.Inject(filter, grid);
        }

        /// <summary>
        /// Invoke the filter.
        /// </summary>
        /// <param name="input">Input.</param>
        /// <returns></returns>
        public int Invoke(IBinaryStream input)
        {
            var rawReader = _ignite.Marshaller.StartUnmarshal(input).GetRawReader();

            var nodeId = rawReader.ReadGuid();

            Debug.Assert(nodeId != null);

            return _invoker(nodeId.Value, rawReader.ReadObject<object>()) ? 1 : 0;
        }

        /// <summary>
        /// Wrapped <see cref="IMessageListener{T}" />.
        /// </summary>
        public object Filter
        {
            get { return _filter; }
        }

        /// <summary>
        /// Destroy callback.
        /// </summary>
        public Action DestroyAction { private get; set; }

        /** <inheritDoc /> */
        public void Release()
        {
            if (DestroyAction != null)
                DestroyAction();
        }

        /// <summary>
        /// Creates local holder instance.
        /// </summary>
        /// <param name="grid">Ignite instance.</param>
        /// <param name="listener">Filter.</param>
        /// <returns>
        /// New instance of <see cref="MessageListenerHolder" />
        /// </returns>
        public static MessageListenerHolder CreateLocal<T>(Ignite grid, IMessageListener<T> listener)
        {
            Debug.Assert(listener != null);

            return new MessageListenerHolder(grid, listener, (id, msg) => listener.Invoke(id, (T)msg));
        }

        /// <summary>
        /// Creates remote holder instance.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="memPtr">Memory pointer.</param>
        /// <returns>Deserialized instance of <see cref="MessageListenerHolder"/></returns>
        public static MessageListenerHolder CreateRemote(Ignite grid, long memPtr)
        {
            Debug.Assert(grid != null);

            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                return grid.Marshaller.Unmarshal<MessageListenerHolder>(stream);
            }
        }

        /// <summary>
        /// Gets the invoker func.
        /// </summary>
        private static Func<Guid, object, bool> GetInvoker(object pred)
        {
            var func = DelegateTypeDescriptor.GetMessageListener(pred.GetType());

            return (id, msg) => func(pred, id, msg);
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter)writer.GetRawWriter();

            writer0.WriteObjectDetached(Filter);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageListenerHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public MessageListenerHolder(BinaryReader reader)
        {
            _filter = reader.ReadObject<object>();

            _invoker = GetInvoker(_filter);

            _ignite = reader.Marshaller.Ignite;

            ResourceProcessor.Inject(_filter, _ignite);
        }
    }
}
