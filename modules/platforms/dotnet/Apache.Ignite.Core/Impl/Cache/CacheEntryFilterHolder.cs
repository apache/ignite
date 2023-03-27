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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Platform;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Non-generic binary filter wrapper.
    /// </summary>
    internal sealed class CacheEntryFilterHolder : IBinaryWriteAware
    {
        /** Wrapped ICacheEntryFilter */
        private readonly object _pred;

        /** Invoker function that takes key and value and invokes wrapped ICacheEntryFilter */
        private readonly Func<object, object, bool> _invoker;
        
        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Grid. */
        private readonly Marshaller _marsh;

        /** Platform cache. When not null, only the key is passed to the filter. */
        private IPlatformCache _platformCache;

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

            var key = rawReader.ReadObject<object>();
            var hasVal = rawReader.ReadBoolean();
            object val;

            if (hasVal)
            {
                val = rawReader.ReadObject<object>();
            }
            else
            {
                // Platform cache on primary node is always up-to-date with actual cache entry in Java,
                // so we can use value from platform cache for filtering.
                if (_platformCache == null || !_platformCache.TryGetValue(key, out val))
                {
                    // Request value from Java.
                    // This should be rare, because primary keys are always in platform cache.
                    val = _marsh.Ignite.GetJavaThreadLocal();
                }
            }

            return _invoker(key, val) ? 1 : 0;
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

                var filterHolder = grid.Marshaller.Unmarshal<CacheEntryFilterHolder>(stream);

                if (stream.ReadBool())
                {
                    var cacheId = stream.ReadInt();
                    filterHolder._platformCache = grid.PlatformCacheManager.TryGetPlatformCache(cacheId);
                }

                return filterHolder;
            }
        }
    }
}
