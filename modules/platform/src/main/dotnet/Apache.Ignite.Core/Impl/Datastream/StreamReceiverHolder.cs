/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Cache;
    using GridGain.Datastream;
    using GridGain.Impl.Cache;
    using GridGain.Impl.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;
    using GridGain.Portable;

    /// <summary>
    /// Portable wrapper for <see cref="IStreamReceiver{K,V}"/>.
    /// </summary>
    internal class StreamReceiverHolder : IPortableWriteAware
    {
        /** */
        private const byte RCV_NORMAL = 0;

        /** */
        public const byte RCV_TRANSFORMER = 1;

        /** Generic receiver. */
        private readonly object rcv;
        
        /** Invoker delegate. */
        private readonly Action<object, GridImpl, IUnmanagedTarget, IPortableStream, bool> invoke;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamReceiverHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public StreamReceiverHolder(PortableReaderImpl reader)
        {
            var rcvType = reader.ReadByte();

            rcv = PortableUtils.ReadPortableOrSerializable<object>(reader);
            
            Debug.Assert(rcv != null);

            var type = rcv.GetType();

            if (rcvType == RCV_TRANSFORMER)
            {
                // rcv is a user ICacheEntryProcessor<K, V, A, R>, construct StreamTransformer from it.
                // (we can't marshal StreamTransformer directly, because it is generic, 
                // and we do not know type arguments that user will have)
                rcv = DelegateTypeDescriptor.GetStreamTransformerCtor(type)(rcv);
            }

            invoke = DelegateTypeDescriptor.GetStreamReceiver(rcv.GetType());
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamReceiverHolder"/> class.
        /// </summary>
        /// <param name="rcv">Receiver.</param>
        /// <param name="invoke">Invoke delegate.</param>
        public StreamReceiverHolder(object rcv, 
            Action<object, GridImpl, IUnmanagedTarget, IPortableStream, bool> invoke)
        {
            Debug.Assert(rcv != null);
            Debug.Assert(invoke != null);

            this.rcv = rcv;
            this.invoke = invoke;
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var w = writer.RawWriter();

            var writeAware = rcv as IPortableWriteAware;

            if (writeAware != null)
                writeAware.WritePortable(writer);
            else
            {
                w.WriteByte(RCV_NORMAL);
                PortableUtils.WritePortableOrSerializable((PortableWriterImpl) writer, rcv);
            }
        }

        /// <summary>
        /// Updates cache with batch of entries.
        /// </summary>
        /// <param name="grid">The grid.</param>
        /// <param name="cache">Cache.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Portable flag.</param>
        public void Receive(GridImpl grid, IUnmanagedTarget cache, IPortableStream stream, bool keepPortable)
        {
            Debug.Assert(grid != null);
            Debug.Assert(cache != null);
            Debug.Assert(stream != null);

            invoke(rcv, grid, cache, stream, keepPortable);
        }

        /// <summary>
        /// Invokes the receiver.
        /// </summary>
        /// <param name="receiver">Receiver.</param>
        /// <param name="grid">Grid.</param>
        /// <param name="cache">Cache.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="keepPortable">Portable flag.</param>
        public static void InvokeReceiver<K, V>(IStreamReceiver<K, V> receiver, GridImpl grid, IUnmanagedTarget cache,
            IPortableStream stream, bool keepPortable)
        {
            var reader = grid.Marshaller.StartUnmarshal(stream, keepPortable);

            var size = reader.ReadInt();

            var entries = new List<ICacheEntry<K, V>>(size);

            for (var i = 0; i < size; i++)
                entries.Add(new CacheEntry<K, V>(reader.ReadObject<K>(), reader.ReadObject<V>()));

            receiver.Receive(grid.Cache<K, V>(cache, keepPortable), entries);
        }
    }
}