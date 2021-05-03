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

namespace Apache.Ignite.Core.Client.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Thin client data streamer.
    /// </summary>
    public interface IDataStreamerClient<TK, TV> : IDisposable
    {
        string CacheName { get; }

        DataStreamerClientOptions<TK, TV> Options { get; }

        // TODO: Handle removals when val is null.
        /// <summary>
        /// Adds an entry to the streamer.
        /// <para />
        /// This method adds an entry to the buffer - it does not block the thread and does not perform IO.
        /// When the buffer gets full, it is scheduled for asynchronous flush.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        void Add(TK key, TV val);

        void Add(IEnumerable<KeyValuePair<TK, TV>> entries);

        void Remove(TK key);

        void Remove(IEnumerable<TK> keys);

        void Flush();

        Task FlushAsync();

        /// <summary>
        /// Closes this streamer, optionally loading any remaining data into the cache.
        /// </summary>
        /// <param name="cancel">Whether to cancel ongoing loading operations. When set to <c>true</c>,
        /// there is no guarantee which part of remaining data will be actually loaded into the cache.</param>
        void Close(bool cancel);

        /// <summary>
        /// Closes this streamer, optionally loading any remaining data into the cache.
        /// </summary>
        /// <param name="cancel">Whether to cancel ongoing loading operations. When set to <c>true</c>,
        /// there is no guarantee which part of remaining data will be actually loaded into the cache.</param>
        Task CloseAsync(bool cancel);
    }
}
