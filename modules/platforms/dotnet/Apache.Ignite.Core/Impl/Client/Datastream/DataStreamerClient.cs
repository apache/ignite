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

namespace Apache.Ignite.Core.Impl.Client.Datastream
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client.Datastream;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    ///
    /// </summary>
    internal class DataStreamerClient<TK, TV> : IDataStreamerClient<TK, TV>
    {
        /** */
        private readonly IgniteClient _client;

        /** */
        private readonly int _cacheId;

        /** */
        private readonly string _cacheName;

        /** */
        private readonly DataStreamerClientOptions<TK, TV> _options;

        /** TODO: Handle removals for value types */
        private readonly ConcurrentQueue<KeyValuePair<TK, TV>> _entries = new ConcurrentQueue<KeyValuePair<TK, TV>>();

        public DataStreamerClient(IgniteClient client, string cacheName, DataStreamerClientOptions<TK, TV> options)
        {
            Debug.Assert(client != null);
            Debug.Assert(!string.IsNullOrEmpty(cacheName));

            _client = client;
            _cacheName = cacheName;
            _cacheId = BinaryUtils.GetCacheId(cacheName);

            // Copy to prevent modification.
            _options = new DataStreamerClientOptions<TK, TV>(options);
        }

        public void Dispose()
        {
            // TODO: Dispose should not throw - how can we achieve that?
            // Require Flush, like Transaction requires Commit?
            // Log errors, but don't throw?
            _client.Socket.DoOutInOp(ClientOp.DataStreamerStart, ctx =>
            {
                var w = ctx.Writer;

                w.WriteInt(_cacheId);
                w.WriteByte(0x10); // Close
                w.WriteInt(_options.ServerPerNodeBufferSize); // PerNodeBufferSize
                w.WriteInt(_options.ServerPerThreadBufferSize); // PerThreadBufferSize
                w.WriteObject(_options.Receiver); // Receiver

                w.WriteInt(_entries.Count);

                foreach (var entry in _entries)
                {
                    w.WriteObjectDetached(entry.Key);
                    w.WriteObjectDetached(entry.Value);
                }
            }, ctx => ctx.Stream.ReadLong());
        }

        /** <inheritdoc /> */
        public string CacheName
        {
            get { return _cacheName; }
        }

        public DataStreamerClientOptions<TK, TV> Options
        {
            get
            {
                // Copy to prevent modification.
                return new DataStreamerClientOptions<TK, TV>(_options);
            }
        }

        public void Add(TK key, TV val)
        {
            _entries.Enqueue(new KeyValuePair<TK, TV>(key, val));
        }

        public void Add(IEnumerable<KeyValuePair<TK, TV>> entries)
        {
            throw new System.NotImplementedException();
        }

        public void Remove(TK key)
        {
            throw new System.NotImplementedException();
        }

        public void Remove(IEnumerable<TK> keys)
        {
            throw new System.NotImplementedException();
        }

        public void Flush()
        {
            throw new System.NotImplementedException();
        }

        public Task FlushAsync()
        {
            throw new System.NotImplementedException();
        }
    }
}
