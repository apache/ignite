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

namespace Apache.Ignite.Core.Impl.Cache.Store
{
    using System.Collections;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Interop cache store.
    /// </summary>
    internal class CacheStore
    {
        /** */
        private const byte OpLoadCache = 0;

        /** */
        private const byte OpLoad = 1;

        /** */
        private const byte OpLoadAll = 2;

        /** */
        private const byte OpPut = 3;

        /** */
        private const byte OpPutAll = 4;

        /** */
        private const byte OpRmv = 5;

        /** */
        private const byte OpRmvAll = 6;

        /** */
        private const byte OpSesEnd = 7;
        
        /** */
        private readonly bool _convertPortable;

        /** Store. */
        private readonly ICacheStore _store;

        /** Session. */
        private readonly CacheStoreSessionProxy _sesProxy;

        /** */
        private readonly long _handle;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheStore" /> class.
        /// </summary>
        /// <param name="store">Store.</param>
        /// <param name="convertPortable">Whether to convert portable objects.</param>
        /// <param name="registry">The handle registry.</param>
        private CacheStore(ICacheStore store, bool convertPortable, HandleRegistry registry)
        {
            Debug.Assert(store != null);

            _store = store;
            _convertPortable = convertPortable;

            _sesProxy = new CacheStoreSessionProxy();

            ResourceProcessor.InjectStoreSession(store, _sesProxy);

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
        internal static CacheStore CreateInstance(long memPtr, HandleRegistry registry)
        {
            using (var stream = IgniteManager.Memory.Get(memPtr).Stream())
            {
                var reader = PortableUtils.Marshaller.StartUnmarshal(stream, PortableMode.KeepPortable);

                var assemblyName = reader.ReadString();
                var className = reader.ReadString();
                var convertPortable = reader.ReadBoolean();
                var propertyMap = reader.ReadGenericDictionary<string, object>();

                var store = (ICacheStore) IgniteUtils.CreateInstance(assemblyName, className);

                IgniteUtils.SetProperties(store, propertyMap);

                return new CacheStore(store, convertPortable, registry);
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
            ResourceProcessor.Inject(_store, grid);
        }

        /// <summary>
        /// Invokes a store operation.
        /// </summary>
        /// <param name="input">Input stream.</param>
        /// <param name="cb">Callback.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>Invocation result.</returns>
        /// <exception cref="IgniteException">Invalid operation type:  + opType</exception>
        public int Invoke(IPortableStream input, IUnmanagedTarget cb, Ignite grid)
        {
            IPortableReader reader = grid.Marshaller.StartUnmarshal(input,
                _convertPortable ? PortableMode.Deserialize : PortableMode.ForcePortable);
            
            IPortableRawReader rawReader = reader.RawReader();

            int opType = rawReader.ReadByte();

            // Setup cache sessoin for this invocation.
            long sesId = rawReader.ReadLong();
            
            CacheStoreSession ses = grid.HandleRegistry.Get<CacheStoreSession>(sesId, true);

            ses.CacheName = rawReader.ReadString();

            _sesProxy.SetSession(ses);

            try
            {
                // Perform operation.
                switch (opType)
                {
                    case OpLoadCache:
                        _store.LoadCache((k, v) => WriteObjects(cb, grid, k, v), rawReader.ReadObjectArray<object>());

                        break;

                    case OpLoad:
                        object val = _store.Load(rawReader.ReadObject<object>());

                        if (val != null)
                            WriteObjects(cb, grid, val);

                        break;

                    case OpLoadAll:
                        var keys = rawReader.ReadCollection();

                        var result = _store.LoadAll(keys);

                        foreach (DictionaryEntry entry in result)
                            WriteObjects(cb, grid, entry.Key, entry.Value);

                        break;

                    case OpPut:
                        _store.Write(rawReader.ReadObject<object>(), rawReader.ReadObject<object>());

                        break;

                    case OpPutAll:
                        _store.WriteAll(rawReader.ReadDictionary());

                        break;

                    case OpRmv:
                        _store.Delete(rawReader.ReadObject<object>());

                        break;

                    case OpRmvAll:
                        _store.DeleteAll(rawReader.ReadCollection());

                        break;

                    case OpSesEnd:
                        grid.HandleRegistry.Release(sesId);

                        _store.SessionEnd(rawReader.ReadBoolean());

                        break;

                    default:
                        throw new IgniteException("Invalid operation type: " + opType);
                }

                return 0;
            }
            finally
            {
                _sesProxy.ClearSession();
            }
        }

        /// <summary>
        /// Writes objects to the marshaller.
        /// </summary>
        /// <param name="cb">Optional callback.</param>
        /// <param name="grid">Grid.</param>
        /// <param name="objects">Objects.</param>
        private static void WriteObjects(IUnmanagedTarget cb, Ignite grid, params object[] objects)
        {
            using (var stream = IgniteManager.Memory.Allocate().Stream())
            {
                PortableWriterImpl writer = grid.Marshaller.StartMarshal(stream);

                try
                {
                    foreach (var obj in objects)
                    {
                        writer.DetachNext();
                        writer.WriteObject(obj);
                    }
                }
                finally
                {
                    grid.Marshaller.FinishMarshal(writer);
                }

                if (cb != null)
                {
                    stream.SynchronizeOutput();

                    UnmanagedUtils.CacheStoreCallbackInvoke(cb, stream.MemoryPointer);
                }
            }
        }
    }
}
