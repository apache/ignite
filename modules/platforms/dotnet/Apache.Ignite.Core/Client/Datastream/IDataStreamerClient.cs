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
    using System.Threading.Tasks;

    /// <summary>
    /// Thin client data streamer.
    /// <para />
    /// Data streamer is an efficient and fault-tolerant way to load data into cache. Updates are buffered and mapped
    /// to primary nodes to ensure minimal data movement and optimal resource utilization.
    /// Update failures caused by cluster topology changes are retried automatically.
    /// <para />
    /// Note that streamer send data to remote nodes asynchronously, so cache updates can be reordered.
    /// <para />
    /// Instances of the implementing class are thread-safe: data can be added from multiple threads.
    /// <para />
    /// Closing and disposing: <see cref="IDisposable.Dispose"/> method calls <see cref="Close"/><c>(false)</c>.
    /// This will flush any remaining data to the cache synchronously.
    /// To avoid blocking threads when exiting <c>using()</c> block, use <see cref="CloseAsync"/>.
    /// </summary>
    public interface IDataStreamerClient<TK, TV> : IDisposable
    {
        /// <summary>
        /// Gets the cache name.
        /// </summary>
        string CacheName { get; }

        /// <summary>
        /// Gets a value indicating whether this streamer is closed.
        /// </summary>
        bool IsClosed { get; }

        /// <summary>
        /// Gets the options.
        /// </summary>
        DataStreamerClientOptions<TK, TV> Options { get; }

        /// <summary>
        /// Adds an entry to the streamer.
        /// <para />
        /// This method adds an entry to the buffer. When the buffer gets full, it is scheduled for
        /// asynchronous background flush. This method will block when the number of active flush operations
        /// exceeds <see cref="DataStreamerClientOptions.PerNodeParallelOperations"/>.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value. When null, cache entry will be removed.</param>
        void Add(TK key, TV val);

        /// <summary>
        /// Adds a removal entry to the streamer. Cache entry with the specified key will be removed.
        /// <para />
        /// Removal requires <see cref="DataStreamerClientOptions.AllowOverwrite"/> to be <c>true</c>.
        /// <para />
        /// This method adds an entry to the buffer. When the buffer gets full, it is scheduled for
        /// asynchronous background flush. This method will block when the number of active flush operations
        /// exceeds <see cref="DataStreamerClientOptions.PerNodeParallelOperations"/>.
        /// </summary>
        /// <param name="key"></param>
        void Remove(TK key);

        /// <summary>
        /// Flushes all buffered entries.
        /// </summary>
        void Flush();

        /// <summary>
        /// Flushes all buffered entries asynchronously.
        /// </summary>
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
