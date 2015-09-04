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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Non-generic portable filter wrapper.
    /// </summary>
    internal class CacheEntryFilterHolder : IPortableWriteAware
    {
        /** Wrapped ICacheEntryFilter */
        private readonly object _pred;

        /** Invoker function that takes key and value and invokes wrapped ICacheEntryFilter */
        private readonly Func<object, object, bool> _invoker;
        
        /** Keep portable flag. */
        private readonly bool _keepPortable;

        /** Grid. */
        private readonly PortableMarshaller _marsh;
        
        /** Handle. */
        private readonly long _handle;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryFilterHolder" /> class.
        /// </summary>
        /// <param name="pred">The <see cref="ICacheEntryFilter{TK,TV}" /> to wrap.</param>
        /// <param name="invoker">The invoker func that takes key and value and invokes wrapped ICacheEntryFilter.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public CacheEntryFilterHolder(object pred, Func<object, object, bool> invoker, PortableMarshaller marsh, 
            bool keepPortable)
        {
            Debug.Assert(pred != null);
            Debug.Assert(invoker != null);
            Debug.Assert(marsh != null);

            _pred = pred;
            _invoker = invoker;
            _marsh = marsh;
            _keepPortable = keepPortable;

            _handle = marsh.Ignite.HandleRegistry.Allocate(this);
        }

        /// <summary>
        /// Gets the handle.
        /// </summary>
        public long Handle
        {
            get { return _handle; }
        }

        /// <summary>
        /// Invokes the cache filter.
        /// </summary>
        /// <param name="input">The input stream.</param>
        /// <returns>Invocation result.</returns>
        public int Invoke(IPortableStream input)
        {
            var rawReader = _marsh.StartUnmarshal(input, _keepPortable).RawReader();

            return _invoker(rawReader.ReadObject<object>(), rawReader.ReadObject<object>()) ? 1 : 0;
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, _pred);
            
            writer0.WriteBoolean(_keepPortable);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public CacheEntryFilterHolder(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            _pred = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            _keepPortable = reader0.ReadBoolean();

            _marsh = reader0.Marshaller;

            _invoker = GetInvoker(_pred);

            _handle = _marsh.Ignite.HandleRegistry.Allocate(this);
        }

        /// <summary>
        /// Gets the invoker func.
        /// </summary>
        private static Func<object, object, bool> GetInvoker(object pred)
        {
            var func = DelegateTypeDescriptor.GetCacheEntryFilter(pred.GetType());

            return (key, val) => func(pred, key, val);
        }

        /// <summary>
        /// Creates an instance of this class from a stream.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>Deserialized instance of <see cref="CacheEntryFilterHolder"/></returns>
        public static CacheEntryFilterHolder CreateInstance(long memPtr, Ignite grid)
        {
            var stream = IgniteManager.Memory.Get(memPtr).Stream();

            Debug.Assert(grid != null);

            var marsh = grid.Marshaller;

            return marsh.Unmarshal<CacheEntryFilterHolder>(stream);
        }
    }
}
