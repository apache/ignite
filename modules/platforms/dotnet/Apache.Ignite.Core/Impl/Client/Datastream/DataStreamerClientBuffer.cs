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
    using System.Threading;

    /// <summary>
    /// Client data streamer buffer.
    /// </summary>
    internal sealed class DataStreamerClientBuffer<TK, TV>
    {
        /** Concurrent bag already has per-thread buffers. */
        private readonly ConcurrentBag<DataStreamerClientEntry<TK, TV>> _entries = 
            new ConcurrentBag<DataStreamerClientEntry<TK, TV>>();

        /** */
        private readonly int _maxSize;

        /** */
        private int _size;

        public DataStreamerClientBuffer(int maxSize)
        {
            _maxSize = maxSize;
        }

        public bool Add(TK key, TV val)
        {
            if (Interlocked.Increment(ref _size) > _maxSize)
            {
                return false;
            }
            
            _entries.Add(new DataStreamerClientEntry<TK, TV>(key, val));

            return true;
        }
        
        public bool Remove(TK key)
        {
            if (Interlocked.Increment(ref _size) > _maxSize)
            {
                return false;
            }
            
            _entries.Add(new DataStreamerClientEntry<TK, TV>(key));

            return true;
        }
    }
}