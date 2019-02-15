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

namespace Apache.Ignite.Core.Impl.Cache
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Non-generic binary filter wrapper.
    /// </summary>
    internal class CacheEntryFilterHolder : IBinaryWriteAware
    {
        /** Wrapped ICacheEntryFilter */
        private readonly object _pred;

        /** Invoker function that takes key and value and invokes wrapped ICacheEntryFilter */
        private readonly Func<object, object, bool> _invoker;
        
        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Grid. */
        private readonly Marshaller _marsh;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryFilterHolder" /> class.
        /// </summary>
        /// <param name="pred">The <see cref="ICacheEntryFilter{TK,TV}" /> to wrap.</param>
        /// <param name="invoker">The invoker func that takes key and value and invokes wrapped ICacheEntryFilter.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        public CacheEntryFilterHolder(object pred, Func<object, object, bool> invoker, Marshaller marsh, 
            bool keepBinary)
        {
            Debug.Assert(pred != null);
            Debug.Assert(invoker != null);
            Debug.Assert(marsh != null);

            _pred = pred;
            _invoker = invoker;
            _marsh = marsh;
            _keepBinary = keepBinary;

            InjectResources();
        }

        /// <summary>
        /// Invokes the cache filter.
        /// </summary>
        /// <param name="input">The input stream.</param>
        /// <returns>Invocation result.</returns>
        public int Invoke(IBinaryStream input)
        {
            var rawReader = _marsh.StartUnmarshal(input, _keepBinary).GetRawReader();

            return _invoker(rawReader.ReadObject<object>(), rawReader.ReadObject<object>()) ? 1 : 0;
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter)writer.GetRawWriter();

            writer0.WriteObjectDetached(_pred);
            
            writer0.WriteBoolean(_keepBinary);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryFilterHolder"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public CacheEntryFilterHolder(BinaryReader reader)
        {
            _pred = reader.ReadObject<object>();

            _keepBinary = reader.ReadBoolean();

            _marsh = reader.Marshaller;

            _invoker = GetInvoker(_pred);

            InjectResources();
        }

        /// <summary>
        /// Injects the resources.
        /// </summary>
        private void InjectResources()
        {
            ResourceProcessor.Inject(_pred, _marsh.Ignite);
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
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                Debug.Assert(grid != null);

                var marsh = grid.Marshaller;

                return marsh.Unmarshal<CacheEntryFilterHolder>(stream);
            }
        }
    }
}
