/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
        private const byte OP_LOAD_CACHE = 0;

        /** */
        private const byte OP_LOAD = 1;

        /** */
        private const byte OP_LOAD_ALL = 2;

        /** */
        private const byte OP_PUT = 3;

        /** */
        private const byte OP_PUT_ALL = 4;

        /** */
        private const byte OP_RMV = 5;

        /** */
        private const byte OP_RMV_ALL = 6;

        /** */
        private const byte OP_SES_END = 7;
        
        /** */
        private readonly bool convertPortable;

        /** Store. */
        private readonly ICacheStore store;

        /** Session. */
        private readonly CacheStoreSessionProxy sesProxy;

        /** */
        private readonly long handle;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheStore" /> class.
        /// </summary>
        /// <param name="store">Store.</param>
        /// <param name="convertPortable">Whether to convert portable objects.</param>
        /// <param name="registry">The handle registry.</param>
        private CacheStore(ICacheStore store, bool convertPortable, HandleRegistry registry)
        {
            Debug.Assert(store != null);

            this.store = store;
            this.convertPortable = convertPortable;

            sesProxy = new CacheStoreSessionProxy();

            ResourceProcessor.InjectStoreSession(store, sesProxy);

            handle = registry.AllocateCritical(this);
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
            using (var stream = GridManager.Memory.Get(memPtr).Stream())
            {
                var reader = PortableUtils.Marshaller.StartUnmarshal(stream, PortableMode.KEEP_PORTABLE);

                var assemblyName = reader.ReadString();
                var className = reader.ReadString();
                var convertPortable = reader.ReadBoolean();
                var propertyMap = reader.ReadGenericDictionary<string, object>();

                var store = (ICacheStore) GridUtils.CreateInstance(assemblyName, className);

                GridUtils.SetProperties(store, propertyMap);

                return new CacheStore(store, convertPortable, registry);
            }
        }

        /// <summary>
        /// Gets the handle.
        /// </summary>
        public long Handle
        {
            get { return handle; }
        }

        /// <summary>
        /// Initializes this instance with a grid.
        /// </summary>
        /// <param name="grid">Grid.</param>
        public void Init(GridImpl grid)
        {
            ResourceProcessor.Inject(store, grid);
        }

        /// <summary>
        /// Invokes a store operation.
        /// </summary>
        /// <param name="input">Input stream.</param>
        /// <param name="cb">Callback.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>Invocation result.</returns>
        /// <exception cref="IgniteException">Invalid operation type:  + opType</exception>
        public int Invoke(IPortableStream input, IUnmanagedTarget cb, GridImpl grid)
        {
            IPortableReader reader = grid.Marshaller.StartUnmarshal(input,
                convertPortable ? PortableMode.DESERIALIZE : PortableMode.FORCE_PORTABLE);
            
            IPortableRawReader rawReader = reader.RawReader();

            int opType = rawReader.ReadByte();

            // Setup cache sessoin for this invocation.
            long sesId = rawReader.ReadLong();
            
            CacheStoreSession ses = grid.HandleRegistry.Get<CacheStoreSession>(sesId, true);

            ses.CacheName = rawReader.ReadString();

            sesProxy.SetSession(ses);

            try
            {
                // Perform operation.
                switch (opType)
                {
                    case OP_LOAD_CACHE:
                        store.LoadCache((k, v) => WriteObjects(cb, grid, k, v), rawReader.ReadObjectArray<object>());

                        break;

                    case OP_LOAD:
                        object val = store.Load(rawReader.ReadObject<object>());

                        if (val != null)
                            WriteObjects(cb, grid, val);

                        break;

                    case OP_LOAD_ALL:
                        var keys = rawReader.ReadCollection();

                        var result = store.LoadAll(keys);

                        foreach (DictionaryEntry entry in result)
                            WriteObjects(cb, grid, entry.Key, entry.Value);

                        break;

                    case OP_PUT:
                        store.Write(rawReader.ReadObject<object>(), rawReader.ReadObject<object>());

                        break;

                    case OP_PUT_ALL:
                        store.WriteAll(rawReader.ReadDictionary());

                        break;

                    case OP_RMV:
                        store.Delete(rawReader.ReadObject<object>());

                        break;

                    case OP_RMV_ALL:
                        store.DeleteAll(rawReader.ReadCollection());

                        break;

                    case OP_SES_END:
                        grid.HandleRegistry.Release(sesId);

                        store.SessionEnd(rawReader.ReadBoolean());

                        break;

                    default:
                        throw new IgniteException("Invalid operation type: " + opType);
                }

                return 0;
            }
            finally
            {
                sesProxy.ClearSession();
            }
        }

        /// <summary>
        /// Writes objects to the marshaller.
        /// </summary>
        /// <param name="cb">Optional callback.</param>
        /// <param name="grid">Grid.</param>
        /// <param name="objects">Objects.</param>
        private static void WriteObjects(IUnmanagedTarget cb, GridImpl grid, params object[] objects)
        {
            using (var stream = GridManager.Memory.Allocate().Stream())
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
