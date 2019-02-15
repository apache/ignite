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