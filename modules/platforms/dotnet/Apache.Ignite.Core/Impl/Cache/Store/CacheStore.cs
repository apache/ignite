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

namespace Apache.Ignite.Core.Impl.Cache.Store
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Interop cache store, delegates to generic <see cref="CacheStoreInternal{TK,TV}"/> wrapper.
    /// </summary>
    internal class CacheStore
    {
        /** Store. */
        private readonly ICacheStoreInternal _store;

        /** */
        private readonly long _handle;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheStore" /> class.
        /// </summary>
        /// <param name="store">Store.</param>
        /// <param name="registry">The handle registry.</param>
        private CacheStore(ICacheStoreInternal store, HandleRegistry registry)
        {
            Debug.Assert(store != null);

            _store = store;

            _handle = registry.AllocateCritical(this);
        }

        /// <summary>
        /// Creates interop cache store from a stream.
        /// </summary>
        /// <param name="memPtr">Memory pointer.</param>
        /// <param name="registry">The handle registry.</param>
        /// <returns>
        /// Interop cache store.
        /// </returns>
        public static CacheStore CreateInstance(long memPtr, HandleRegistry registry)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).GetStream())
            {
                var reader = BinaryUtils.Marshaller.StartUnmarshal(stream);

                var convertBinary = reader.ReadBoolean();
                var factory = reader.ReadObject<IFactory<ICacheStore>>();

                ICacheStore store;

                if (factory != null)
                {
                    store = factory.CreateInstance();

                    if (store == null)
                    {
                        throw new IgniteException("Cache store factory should not return null: " + factory.GetType());
                    }
                }
                else
                {
                    var className = reader.ReadString();
                    var propertyMap = reader.ReadDictionaryAsGeneric<string, object>();

                    store = IgniteUtils.CreateInstance<ICacheStore>(className, propertyMap);
                }

                var iface = GetCacheStoreInterface(store);

                var storeType = typeof(CacheStoreInternal<,>).MakeGenericType(iface.GetGenericArguments());

                var storeInt = (ICacheStoreInternal)Activator.CreateInstance(storeType, store, convertBinary);

                return new CacheStore(storeInt, registry);
            }
        }

        /// <summary>
        /// Gets the handle.
        /// </summary>
        public long Handle
        {
            get { return _handle; }
        }

        /// <summary>
        /// Initializes this instance with a grid.
        /// </summary>
        /// <param name="grid">Grid.</param>
        public void Init(Ignite grid)
        {
            _store.Init(grid);
        }

        /// <summary>
        /// Invokes a store operation.
        /// </summary>
        /// <param name="stream">Input stream.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>Invocation result.</returns>
        /// <exception cref="IgniteException">Invalid operation type:  + opType</exception>
        public long Invoke(PlatformMemoryStream stream, Ignite grid)
        {
            return _store.Invoke(stream, grid);
        }
                
        /// <summary>
        /// Gets the generic <see cref="ICacheStore{TK,TV}"/> interface type.
        /// </summary>
        private static Type GetCacheStoreInterface(ICacheStore store)
        {
            var ifaces = store.GetType().GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICacheStore<,>))
                .ToArray();

            if (ifaces.Length == 0)
            {
                throw new IgniteException(string.Format(
                    CultureInfo.InvariantCulture, "Cache store should implement generic {0} interface: {1}",
                    typeof(ICacheStore<,>), store.GetType()));
            }

            if (ifaces.Length > 1)
            {
                throw new IgniteException(string.Format(
                    CultureInfo.InvariantCulture, "Cache store should not implement generic {0} " +
                                                  "interface more than once: {1}",
                    typeof(ICacheStore<,>), store.GetType()));
            }

            return ifaces[0];
        }
    }
}