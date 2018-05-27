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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Generic cache store wrapper.
    /// </summary>
    internal class CacheStoreInternal<TK, TV> : ICacheStoreInternal
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
        private readonly bool _convertBinary;

        /** User store. */
        private readonly ICacheStore<TK, TV> _store;
                
        /** Session. */
        private readonly CacheStoreSessionProxy _sesProxy;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheStoreInternal{TK,TV}"/> class.
        /// </summary>
        public CacheStoreInternal(ICacheStore<TK, TV> store, bool convertBinary)
        {
            Debug.Assert(store != null);

            _store = store;

            _convertBinary = convertBinary;
            
            _sesProxy = new CacheStoreSessionProxy();

            ResourceProcessor.InjectStoreSession(store, _sesProxy);
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
        /// <param name="stream">Input stream.</param>
        /// <param name="grid">Grid.</param>
        /// <returns>Invocation result.</returns>
        /// <exception cref="IgniteException">Invalid operation type:  + opType</exception>
        public int Invoke(IBinaryStream stream, Ignite grid)
        {
            IBinaryReader reader = grid.Marshaller.StartUnmarshal(stream,
                _convertBinary ? BinaryMode.Deserialize : BinaryMode.ForceBinary);

            IBinaryRawReader rawReader = reader.GetRawReader();

            int opType = rawReader.ReadByte();

            // Setup cache session for this invocation.
            long sesId = rawReader.ReadLong();

            CacheStoreSession ses = grid.HandleRegistry.Get<CacheStoreSession>(sesId, true);

            // Session cache name may change in cross-cache transaction.
            // Single session is used for all stores in cross-cache transactions.
            ses.CacheName = rawReader.ReadString();

            _sesProxy.SetSession(ses);

            try
            {
                // Perform operation.
                switch (opType)
                {
                    case OpLoadCache:
                    {
                        var args = rawReader.ReadArray<object>();

                        stream.Seek(0, SeekOrigin.Begin);

                        int cnt = 0;
                        stream.WriteInt(cnt); // Reserve space for count.

                        var writer = grid.Marshaller.StartMarshal(stream);

                        _store.LoadCache((k, v) =>
                        {
                            lock (writer) // User-defined store can be multithreaded.
                            {
                                writer.WriteObjectDetached(k);
                                writer.WriteObjectDetached(v);

                                cnt++;
                            }
                        }, args);

                        stream.WriteInt(0, cnt);

                        grid.Marshaller.FinishMarshal(writer);

                        break;
                    }

                    case OpLoad:
                    {
                        var val = _store.Load(rawReader.ReadObject<TK>());

                        stream.Seek(0, SeekOrigin.Begin);

                        var writer = grid.Marshaller.StartMarshal(stream);

                        writer.WriteObject(val);

                        grid.Marshaller.FinishMarshal(writer);

                        break;
                    }

                    case OpLoadAll:
                    {
                        // We can't do both read and write lazily because stream is reused.
                        // Read keys non-lazily, write result lazily.
                        var keys = ReadAllKeys(rawReader);

                        var result = _store.LoadAll(keys);

                        stream.Seek(0, SeekOrigin.Begin);

                        int cnt = 0;
                        stream.WriteInt(cnt); // Reserve space for count.

                        var writer = grid.Marshaller.StartMarshal(stream);

                        foreach (var entry in result)
                        {
                            var entry0 = entry; // Copy modified closure.

                            writer.WriteObjectDetached(entry0.Key);
                            writer.WriteObjectDetached(entry0.Value);

                            cnt++;
                        }

                        stream.WriteInt(0, cnt);

                        grid.Marshaller.FinishMarshal(writer);

                        break;
                    }

                    case OpPut:
                        _store.Write(rawReader.ReadObject<TK>(), rawReader.ReadObject<TV>());

                        break;

                    case OpPutAll:
                        _store.WriteAll(ReadPairs(rawReader));

                        break;

                    case OpRmv:
                        _store.Delete(rawReader.ReadObject<TK>());

                        break;

                    case OpRmvAll:
                        _store.DeleteAll(ReadKeys(rawReader));

                        break;

                    case OpSesEnd:
                    {
                        var commit = rawReader.ReadBoolean();
                        var last = rawReader.ReadBoolean();

                        if (last)
                        {
                            grid.HandleRegistry.Release(sesId);
                        }

                        _store.SessionEnd(commit);

                        break;
                    }

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
        /// Reads key-value pairs.
        /// </summary>
        private static IEnumerable<KeyValuePair<TK, TV>> ReadPairs(IBinaryRawReader rawReader)
        {
            var size = rawReader.ReadInt();

            for (var i = 0; i < size; i++)
            {
                yield return new KeyValuePair<TK, TV>(rawReader.ReadObject<TK>(), rawReader.ReadObject<TV>());
            }
        }

        /// <summary>
        /// Reads the keys.
        /// </summary>
        private static IEnumerable<TK> ReadKeys(IBinaryRawReader reader)
        {
            var cnt = reader.ReadInt();

            for (var i = 0; i < cnt; i++)
            {
                yield return reader.ReadObject<TK>();
            }
        }
        /// <summary>
        /// Reads the keys.
        /// </summary>
        private static ICollection<TK> ReadAllKeys(IBinaryRawReader reader)
        {
            var cnt = reader.ReadInt();
            var res = new List<TK>(cnt);

            for (var i = 0; i < cnt; i++)
            {
                res.Add(reader.ReadObject<TK>());
            }

            return res;
        }
    }
}
