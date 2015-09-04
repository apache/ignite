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

namespace Apache.Ignite.Core.Impl.Messaging
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Non-generic portable filter wrapper.
    /// </summary>
    internal class MessageFilterHolder : IPortableWriteAware, IHandle
    {
        /** Invoker function that takes key and value and invokes wrapped IMessageFilter */
        private readonly Func<Guid, object, bool> _invoker;

        /** Current Ignite instance. */
        private readonly Ignite _ignite;
        
        /** Underlying filter. */
        private readonly object _filter;

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageFilterHolder" /> class.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="filter">The <see cref="IMessageFilter{T}" /> to wrap.</param>
        /// <param name="invoker">The invoker func that takes key and value and invokes wrapped IMessageFilter.</param>
        private MessageFilterHolder(Ignite grid, object filter, Func<Guid, object, bool> invoker)
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
        public int Invoke(IPortableStream input)
        {
            var rawReader = _ignite.Marshaller.StartUnmarshal(input).RawReader();

            var nodeId = rawReader.ReadGuid();

            Debug.Assert(nodeId != null);

            return _invoker(nodeId.Value, rawReader.ReadObject<object>()) ? 1 : 0;
        }

        /// <summary>
        /// Wrapped <see cref="IMessageFilter{T}" />.
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

        /** <inheritDoc /> */
        public bool Released
        {
            get { return false; } // Multiple releases are allowed.
        }

        /// <summary>
        /// Creates local holder instance.
        /// </summary>
        /// <param name="grid">Ignite instance.</param>
        /// <param name="filter">Filter.</param>
        /// <returns>
        /// New instance of <see cref="MessageFilterHolder" />
        /// </returns>
        public static MessageFilterHolder CreateLocal<T>(Ignite grid, IMessageFilter<T> filter)
        {
            Debug.Assert(filter != null);

            return new MessageFilterHolder(grid, filter, (id, msg) => filter.Invoke(id, (T)msg));
        }

        /// <summary>
        /// Creates remote holder instance.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="memPtr">Memory pointer.</param>
        /// <returns>Deserialized instance of <see cref="MessageFilterHolder"/></returns>
        public static MessageFilterHolder CreateRemote(Ignite grid, long memPtr)
        {
            Debug.Assert(grid != null);
            
            var stream = IgniteManager.Memory.Get(memPtr).Stream();

            var holder = grid.Marshaller.Unmarshal<MessageFilterHolder>(stream);

            return holder;
        }

        /// <summary>
        /// Gets the invoker func.
        /// </summary>
        private static Func<Guid, object, bool> GetInvoker(object pred)
        {
            var func = DelegateTypeDescriptor.GetMessageFilter(pred.GetType());

            return (id, msg) => func(pred, id, msg);
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, Filter);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MessageFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public MessageFilterHolder(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            _filter = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            _invoker = GetInvoker(_filter);

            _ignite = reader0.Marshaller.Ignite;

            ResourceProcessor.Inject(_filter, _ignite);
        }
    }
}
