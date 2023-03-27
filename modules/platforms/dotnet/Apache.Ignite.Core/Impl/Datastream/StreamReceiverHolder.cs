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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary wrapper for <see cref="IStreamReceiver{TK,TV}"/>.
    /// </summary>
    internal class StreamReceiverHolder : IBinaryWriteAware
    {
        /** */
        private const byte RcvNormal = 0;

        /** */
        public const byte RcvTransformer = 1;

        /** Generic receiver. */
        private readonly object _rcv;
        
        /** Invoker delegate. */
        private readonly Action<object, Ignite, IPlatformTargetInternal, IBinaryStream, bool> _invoke;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamReceiverHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public StreamReceiverHolder(IBinaryRawReader reader)
        {
            var rcvType = reader.ReadByte();

            _rcv = reader.ReadObject<object>();

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
            Action<object, Ignite, IPlatformTargetInternal, IBinaryStream, bool> invoke)
        {
            Debug.Assert(rcv != null);
            Debug.Assert(invoke != null);

            _rcv = rcv;
            _invoke = invoke;
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var w = writer.GetRawWriter();

            var writeAware = _rcv as IBinaryWriteAware;

            if (writeAware != null)
                writeAware.WriteBinary(writer);
            else
            {
                w.WriteByte(RcvNormal);
                w.WriteObject(_rcv);
            }
        }

        /// <summary>
        /// Updates cache with batch of entries.
        /// </summary>
        /// <param name="grid">The grid.</param>
        /// <param name="cache">Cache.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="keepBinary">Binary flag.</param>
        public void Receive(Ignite grid, IPlatformTargetInternal cache, IBinaryStream stream, bool keepBinary)
        {
            Debug.Assert(grid != null);
            Debug.Assert(cache != null);
            Debug.Assert(stream != null);

            _invoke(_rcv, grid, cache, stream, keepBinary);
        }

        /// <summary>
        /// Invokes the receiver.
        /// </summary>
        /// <param name="receiver">Receiver.</param>
        /// <param name="grid">Grid.</param>
        /// <param name="cache">Cache.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="keepBinary">Binary flag.</param>
        public static void InvokeReceiver<TK, TV>(IStreamReceiver<TK, TV> receiver, Ignite grid, 
            IPlatformTargetInternal cache, IBinaryStream stream, bool keepBinary)
        {
            var reader = grid.Marshaller.StartUnmarshal(stream, keepBinary);

            var size = reader.ReadInt();

            var entries = new List<ICacheEntry<TK, TV>>(size);

            for (var i = 0; i < size; i++)
                entries.Add(new CacheEntry<TK, TV>(reader.ReadObject<TK>(), reader.ReadObject<TV>()));

            receiver.Receive(Ignite.GetCache<TK, TV>(cache, keepBinary), entries);
        }
    }
}