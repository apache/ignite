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

namespace Apache.Ignite.Core.Impl.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable wrapper for <see cref="IStreamReceiver{TK,TV}"/>.
    /// </summary>
    internal class StreamReceiverHolder : IPortableWriteAware
    {
        /** */
        private const byte RcvNormal = 0;

        /** */
        public const byte RcvTransformer = 1;

        /** Generic receiver. */
        private readonly object _rcv;
        
        /** Invoker delegate. */
        private readonly Action<object, Ignite, IUnmanagedTarget, IPortableStream, bool> _invoke;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamReceiverHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public StreamReceiverHolder(PortableReaderImpl reader)
        {
            var rcvType = reader.ReadByte();

            _rcv = PortableUtils.ReadPortableOrSerializable<object>(reader);
            
            Debug.Assert(_rcv != null);

            var type = _rcv.GetType();

            if (rcvType == RcvTransformer)
            {
                // rcv is a user ICacheEntryProcessor<K, V, A, R>, construct StreamTransformer from it.
                // (we can't marshal StreamTransformer directly, because it is generic, 
                // and we do not know type arguments that user will have)
                _rcv = DelegateTypeDescriptor.GetStreamTransformerCtor(type)(_rcv);
            }

            _invoke = DelegateTypeDescriptor.GetStreamReceiver(_rcv.GetType());
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamReceiverHolder"/> class.
        /// </summary>
        /// <param name="rcv">Receiver.</param>
        /// <param name="invoke">Invoke delegate.</param>
        public StreamReceiverHolder(object rcv, 
            Action<object, Ignite, IUnmanagedTarget, IPortableStream, bool> invoke)
        {
            Debug.Assert(rcv != null);
            Debug.Assert(invoke != null);

            _rcv = rcv;
            _invoke = invoke;
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var w = writer.RawWriter();

            var writeAware = _rcv as IPortableWriteAware;

            if (writeAware != null)
                writeAware.WritePortable(writer);
            else
            {
                w.WriteByte(RcvNormal);
                PortableUtils.WritePortableOrSerializable((PortableWriterImpl) writer, _rcv);
            }
        }

        /// <summary>
        /// Updates cache with batch of entries.
        /// </summary>
        /// <param name="grid">The grid.</param>
        /// <param name="cache">Cache.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Portable flag.</param>
        public void Receive(Ignite grid, IUnmanagedTarget cache, IPortableStream stream, bool keepPortable)
        {
            Debug.Assert(grid != null);
            Debug.Assert(cache != null);
            Debug.Assert(stream != null);

            _invoke(_rcv, grid, cache, stream, keepPortable);
        }

        /// <summary>
        /// Invokes the receiver.
        /// </summary>
        /// <param name="receiver">Receiver.</param>
        /// <param name="grid">Grid.</param>
        /// <param name="cache">Cache.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Portable flag.</param>
        public static void InvokeReceiver<TK, TV>(IStreamReceiver<TK, TV> receiver, Ignite grid, IUnmanagedTarget cache,
            IPortableStream stream, bool keepPortable)
        {
            var reader = grid.Marshaller.StartUnmarshal(stream, keepPortable);

            var size = reader.ReadInt();

            var entries = new List<ICacheEntry<TK, TV>>(size);

            for (var i = 0; i < size; i++)
                entries.Add(new CacheEntry<TK, TV>(reader.ReadObject<TK>(), reader.ReadObject<TV>()));

            receiver.Receive(grid.Cache<TK, TV>(cache, keepPortable), entries);
        }
    }
}