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

        /** TODO: Handle removals for value types */
        private readonly ConcurrentQueue<KeyValuePair<TK, TV>> _entries = new ConcurrentQueue<KeyValuePair<TK, TV>>();

        public DataStreamerClient(IgniteClient client, string cacheName)
        {
            _client = client;
            _cacheId = BinaryUtils.GetCacheId(cacheName);
        }

        public void Dispose()
        {
            _client.Socket.DoOutInOp(ClientOp.DataStreamerStart, ctx =>
            {
                var w = ctx.Writer;

                w.WriteInt(_cacheId);
                w.WriteByte(0x10); // Close
                w.WriteInt(512); // PerNodeBufferSize
                w.WriteInt(4096); // PerThreadBufferSize
                w.WriteObject<object>(null); // Receiver

                w.WriteInt(_entries.Count);

                foreach (var entry in _entries)
                {
                    w.WriteObjectDetached(entry.Key);
                    w.WriteObjectDetached(entry.Value);
                }
            }, ctx => ctx.Stream.ReadLong());
        }

        public void AddData(TK key, TV val)
        {
            _entries.Enqueue(new KeyValuePair<TK, TV>(key, val));
        }

        public Task AddDataAsync(TK key, TV val)
        {
            throw new System.NotImplementedException();
        }
    }
}
