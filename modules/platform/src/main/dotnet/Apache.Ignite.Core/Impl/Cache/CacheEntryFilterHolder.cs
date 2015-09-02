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
        private readonly object pred;

        /** Invoker function that takes key and value and invokes wrapped ICacheEntryFilter */
        private readonly Func<object, object, bool> invoker;
        
        /** Keep portable flag. */
        private readonly bool keepPortable;

        /** Grid. */
        private readonly PortableMarshaller marsh;
        
        /** Handle. */
        private readonly long handle;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryFilterHolder" /> class.
        /// </summary>
        /// <param name="pred">The <see cref="ICacheEntryFilter{K,V}" /> to wrap.</param>
        /// <param name="invoker">The invoker func that takes key and value and invokes wrapped ICacheEntryFilter.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Keep portable flag.</param>
        public CacheEntryFilterHolder(object pred, Func<object, object, bool> invoker, PortableMarshaller marsh, 
            bool keepPortable)
        {
            Debug.Assert(pred != null);
            Debug.Assert(invoker != null);
            Debug.Assert(marsh != null);

            this.pred = pred;
            this.invoker = invoker;
            this.marsh = marsh;
            this.keepPortable = keepPortable;

            handle = marsh.Grid.HandleRegistry.Allocate(this);
        }

        /// <summary>
        /// Gets the handle.
        /// </summary>
        public long Handle
        {
            get { return handle; }
        }

        /// <summary>
        /// Invokes the cache filter.
        /// </summary>
        /// <param name="input">The input stream.</param>
        /// <returns>Invocation result.</returns>
        public int Invoke(IPortableStream input)
        {
            var rawReader = marsh.StartUnmarshal(input, keepPortable).RawReader();

            return invoker(rawReader.ReadObject<object>(), rawReader.ReadObject<object>()) ? 1 : 0;
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, pred);
            
            writer0.WriteBoolean(keepPortable);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public CacheEntryFilterHolder(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            pred = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            keepPortable = reader0.ReadBoolean();

            marsh = reader0.Marshaller;

            invoker = GetInvoker(pred);

            handle = marsh.Grid.HandleRegistry.Allocate(this);
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
            var stream = GridManager.Memory.Get(memPtr).Stream();

            Debug.Assert(grid != null);

            var marsh = grid.Marshaller;

            return marsh.Unmarshal<CacheEntryFilterHolder>(stream);
        }
    }
}
