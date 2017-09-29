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

namespace Apache.Ignite.Core.Impl.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Client.Cache.Query;
    using Apache.Ignite.Core.Impl.Common;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Client cache implementation.
    /// </summary>
    internal class CacheClient<TK, TV> : ICacheClient<TK, TV>
    {
        /** Scan query filter platform code: .NET filter. */
        private const byte FilterPlatformDotnet = 2;

        /** Cache name. */
        private readonly string _name;

        /** Cache id. */
        private readonly int _id;

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Keep binary flag. */
        private bool _keepBinary = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClient{TK, TV}" /> class.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="name">Cache name.</param>
        public CacheClient(IgniteClient ignite, string name)
        {
            Debug.Assert(ignite != null);
            Debug.Assert(name != null);

            _name = name;
            _ignite = ignite;
            _marsh = _ignite.Marshaller;
            _id = BinaryUtils.GetCacheId(name);
        }

        /** <inheritDoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritDoc /> */
        public TV this[TK key]
        {
            get { return Get(key); }
            set { Put(key, value); }
        }

        /** <inheritDoc /> */
        public TV Get(TK key)
        {
            IgniteArgumentCheck.NotNull(key, "key");

            return DoOutInOp(ClientOp.CacheGet, w => w.WriteObject(key), UnmarshalNotNull<TV>);
        }

        /** <inheritDoc /> */
        public void Put(TK key, TV val)
        {
            IgniteArgumentCheck.NotNull(key, "key");
            IgniteArgumentCheck.NotNull(val, "val");

            DoOutOp(ClientOp.CachePut, w =>
            {
                w.WriteObjectDetached(key);
                w.WriteObjectDetached(val);
            });
        }

        /** <inheritDoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> Query(ScanQuery<TK, TV> scanQuery)
        {
            IgniteArgumentCheck.NotNull(scanQuery, "query");

            // Filter is a binary object for all platforms.
            // For .NET it is a CacheEntryFilterHolder with a predefined id (BinaryTypeId.CacheEntryPredicateHolder).
            return DoOutInOp(ClientOp.QueryScan, w => WriteScanQuery(w, scanQuery),
                s => new ClientQueryCursor<TK, TV>(_ignite, s.ReadLong(), _keepBinary, s));
        }

        /// <summary>
        /// Does the out in op.
        /// </summary>
        private T DoOutInOp<T>(ClientOp opId, Action<BinaryWriter> writeAction,
            Func<IBinaryStream, T> readFunc)
        {
            return _ignite.Socket.DoOutInOp(opId, stream =>
            {
                stream.WriteInt(_id);
                stream.WriteByte(0);  // Flags (skipStore, etc).

                if (writeAction != null)
                {
                    var writer = _marsh.StartMarshal(stream);

                    writeAction(writer);

                    _marsh.FinishMarshal(writer);
                }
            }, readFunc, HandleError<T>);
        }

        /// <summary>
        /// Does the out op.
        /// </summary>
        private void DoOutOp(ClientOp opId, Action<BinaryWriter> writeAction)
        {
            DoOutInOp<object>(opId, writeAction, null);
        }

        /// <summary>
        /// Unmarshals the value, throwing an exception for nulls.
        /// </summary>
        private T UnmarshalNotNull<T>(IBinaryStream stream)
        {
            var hdr = stream.ReadByte();

            if (hdr == BinaryUtils.HdrNull)
            {
                throw GetKeyNotFoundException();
            }

            stream.Seek(-1, SeekOrigin.Current);

            return _marsh.Unmarshal<T>(stream);
        }

        /// <summary>
        /// Writes the scan query.
        /// </summary>
        private void WriteScanQuery(BinaryWriter writer, ScanQuery<TK, TV> qry)
        {
            Debug.Assert(qry != null);

            if (qry.Filter == null)
            {
                writer.WriteByte(BinaryUtils.HdrNull);
            }
            else
            {
                var holder = new CacheEntryFilterHolder(qry.Filter, (key, val) => qry.Filter.Invoke(
                    new CacheEntry<TK, TV>((TK)key, (TV)val)), writer.Marshaller, _keepBinary);

                writer.WriteObject(holder);

                writer.WriteByte(FilterPlatformDotnet);
            }

            writer.WriteInt(qry.PageSize);

            writer.WriteInt(qry.Partition ?? -1);

            writer.WriteBoolean(qry.Local);
        }

        /// <summary>
        /// Handles the error.
        /// </summary>
        private T HandleError<T>(ClientStatus status, string msg)
        {
            switch (status)
            {
                case ClientStatus.CacheDoesNotExist:
                    throw new IgniteClientException("Cache doesn't exist: " + Name, null, (int) status);

                default:
                    throw new IgniteClientException(msg, null, (int) status);
            }
        }

        /// <summary>
        /// Gets the key not found exception.
        /// </summary>
        private static KeyNotFoundException GetKeyNotFoundException()
        {
            return new KeyNotFoundException("The given key was not present in the cache.");
        }
    }
}
