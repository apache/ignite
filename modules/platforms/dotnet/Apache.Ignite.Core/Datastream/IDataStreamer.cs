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

namespace Apache.Ignite.Core.Datastream
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Data streamer is responsible for loading external data into cache. It achieves it by
    /// properly buffering updates and properly mapping keys to nodes responsible for the data
    /// to make sure that there is the least amount of data movement possible and optimal
    /// network and memory utilization.
    /// <para />
    /// Note that streamer will load data concurrently by multiple internal threads, so the
    /// data may get to remote nodes in different order from which it was added to
    /// the streamer.
    /// <para />
    /// Also note that <c>IDataStreamer</c> is not the only way to load data into cache.
    /// Alternatively you can use 
    /// <see cref="ICacheStore.LoadCache(Action{object, object}, object[])"/>
    /// method to load data from underlying data store. You can also use standard cache
    /// <c>put</c> and <c>putAll</c> operations as well, but they most likely will not perform 
    /// as well as this class for loading data. And finally, data can be loaded from underlying 
    /// data store on demand, whenever it is accessed - for this no explicit data loading step 
    /// is needed.
    /// <para />
    /// <c>IDataStreamer</c> supports the following configuration properties:
    /// <list type="bullet">
    ///     <item>
    ///         <term>PerNodeBufferSize</term>
    ///         <description>When entries are added to data streamer they are not sent to Ignite 
    ///         right away and are buffered internally for better performance and network utilization. 
    ///         This setting controls the size of internal per-node buffer before buffered data is sent to 
    ///         remote node. Default value is 1024.</description>
    ///     </item>
    ///     <item>
    ///         <term>PerNodeParallelOperations</term>
    ///         <description>Sometimes data may be added to the data streamer faster than it can be put 
    ///         in cache. In this case, new buffered load messages are sent to remote nodes before 
    ///         responses from previous ones are received. This could cause unlimited heap memory 
    ///         utilization growth on local and remote nodes. To control memory utilization, this 
    ///         setting limits maximum allowed number of parallel buffered load messages that are 
    ///         being processed on remote nodes. If this number is exceeded, then data streamer add/remove
    ///         methods will block to control memory utilization. Default value is 16.</description>
    ///     </item>
    ///     <item>
    ///         <term>AutoFlushFrequency</term>
    ///         <description>Automatic flush frequency in milliseconds. Essentially, this is the time 
    ///         after which the streamer will make an attempt to submit all data added so far to remote 
    ///         nodes. Note that there is no guarantee that data will be delivered after this concrete 
    ///         attempt (e.g., it can fail when topology is changing), but it won't be lost anyway. 
    ///         Disabled by default (default value is <c>0</c>).</description>
    ///     </item>
    ///     <item>
    ///         <term>Isolated</term>
    ///         <description>Defines if data streamer will assume that there are no other concurrent 
    ///         updates and allow data streamer choose most optimal concurrent implementation. Default value 
    ///         is <c>false</c>.</description>
    ///     </item>
    /// </list>
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IDataStreamer<TK, TV> : IDisposable
    {
        /// <summary>
        /// Name of the cache to load data to.
        /// </summary>
        string CacheName { get; }

        /// <summary>
        /// Flag value indicating that this data streamer assumes that there could be concurrent updates to the cache. 
        /// <para />
        /// Default is <code>false</code>.
        /// </summary>
        bool AllowOverwrite { get; set; }

        /// <summary>
        /// Flag indicating that write-through behavior should be disabled for data loading.
        /// <para />
        /// Default is <code>false</code>.
        /// </summary>
        bool SkipStore { get; set; }

        /// <summary>
        /// Size of per node key-value pairs buffer.
        /// <para />
        /// Setter must be called before any add/remove operation.
        /// <para />
        /// Default is <code>1024</code>.
        /// </summary>
        int PerNodeBufferSize { get; set; }

        /// <summary>
        /// Maximum number of parallel load operations for a single node.
        /// <para />
        /// Setter must be called before any add/remove operation.
        /// <para />
        /// Default is <code>16</code>.
        /// </summary>
        int PerNodeParallelOperations { get; set; }

        /// <summary>
        /// Automatic flush frequency in milliseconds. Essentially, this is the time after which the
        /// streamer will make an attempt to submit all data added so far to remote nodes.
        /// Note that there is no guarantee that data will be delivered after this concrete
        /// attempt (e.g., it can fail when topology is changing), but it won't be lost anyway.
        /// <para />
        /// If set to <code>0</code>, automatic flush is disabled.
        /// <para />
        /// Default is <code>0</code> (disabled).
        /// </summary>
        long AutoFlushFrequency { get; set; }

        /// <summary>
        /// Gets future for this loading process. This future completes whenever method
        /// <see cref="IDataStreamer{K,V}.Close(bool)"/> completes.
        /// </summary>
        IFuture Future { get; }

        /// <summary>
        /// Gets or sets custom stream receiver.
        /// </summary>
        IStreamReceiver<TK, TV> Receiver { get; set; }

        /// <summary>
        /// Adds single key-value pair for loading. Passing <c>null</c> as value will be 
        /// interpreted as removal.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        /// <returns>Future for this operation.</returns>
        IFuture AddData(TK key, TV val);

        /// <summary>
        /// Adds single key-value pair for loading. Passing <c>null</c> as pair's value will 
        /// be interpreted as removal.
        /// </summary>
        /// <param name="pair">Key-value pair.</param>
        /// <returns>Future for this operation.</returns>
        IFuture AddData(KeyValuePair<TK, TV> pair);

        /// <summary>
        /// Adds collection of key-value pairs for loading. 
        /// </summary>
        /// <param name="entries">Entries.</param>
        /// <returns>Future for this operation.</returns>
        IFuture AddData(ICollection<KeyValuePair<TK, TV>> entries);

        /// <summary>
        /// Adds key for removal.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Future for this operation.</returns>
        IFuture RemoveData(TK key);

        /// <summary>
        /// Makes an attempt to load remaining data. This method is mostly similar to 
        /// <see cref="IDataStreamer{K,V}.Flush()"/> with the difference that it won't wait and 
        /// will exit immediately.
        /// </summary>
        void TryFlush();

        /// <summary>
        /// Loads any remaining data, but doesn't close the streamer. Data can be still added after
        /// flush is finished. This method blocks and doesn't allow to add any data until all data
        /// is loaded.
        /// </summary>
        void Flush();

        /// <summary>
        /// Closes this streamer optionally loading any remaining data.
        /// </summary>
        /// <param name="cancel">Whether to cancel ongoing loading operations. When set to <c>true</c>
        /// there is not guarantees what data will be actually loaded to cache.</param>
        void Close(bool cancel);

        /// <summary>
        /// Gets streamer instance with portable mode enabled, changing key and/or value types if necessary.
        /// In portable mode stream receiver gets data in portable format.
        /// You can only change key/value types when transitioning from non-portable to portable streamer;
        /// Changing type of portable streamer is not allowed and will throw an <see cref="InvalidOperationException"/>
        /// </summary>
        /// <typeparam name="TK1">Key type in portable mode.</typeparam>
        /// <typeparam name="TV1">Value type in protable mode.</typeparam>
        /// <returns>Streamer instance with portable mode enabled.</returns>
        IDataStreamer<TK1, TV1> WithKeepPortable<TK1, TV1>();
    }
}
