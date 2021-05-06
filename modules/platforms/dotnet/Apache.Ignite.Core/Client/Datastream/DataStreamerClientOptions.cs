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
    using System.ComponentModel;
    using Apache.Ignite.Core.Datastream;

    /// <summary>
    /// Thin client data streamer options.
    /// <para />
    /// See also <see cref="IDataStreamerClient{TK,TV}"/>, <see cref="IIgniteClient.GetDataStreamer{TK,TV}(string)"/>.
    /// </summary>
    public class DataStreamerClientOptions<TK, TV> // TODO: Think twice: should this be generic?
    {
        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions{TK,TV}"/>.
        /// </summary>
        public DataStreamerClientOptions()
        {
            ServerPerNodeBufferSize = DataStreamerClientDefaults.ServerPerNodeBufferSize;
            ServerPerThreadBufferSize = DataStreamerClientDefaults.ServerPerThreadBufferSize;
            ClientPerNodeBufferSize = DataStreamerClientDefaults.ClientPerNodeBufferSize;
            ClientPerNodeParallelOperations = DataStreamerClientDefaults.ClientPerNodeParallelOperations;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions{TK,TV}"/>.
        /// </summary>
        /// <param name="options">Options to copy from.</param>
        public DataStreamerClientOptions(DataStreamerClientOptions<TK, TV> options) : this()
        {
            if (options == null)
            {
                return;
            }

            // TODO: Auto flush interval
            ReceiverKeepBinary = options.ReceiverKeepBinary;
            Receiver = options.Receiver;
            AllowOverwrite = options.AllowOverwrite;
            SkipStore = options.SkipStore;
            ClientPerNodeBufferSize = options.ClientPerNodeBufferSize;
            ServerPerNodeBufferSize = options.ServerPerNodeBufferSize;
            ServerPerThreadBufferSize = options.ServerPerThreadBufferSize;
            KeepBinary = options.KeepBinary;
            ClientPerNodeParallelOperations = options.ClientPerNodeParallelOperations;
        }

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="Receiver"/> should operate in binary mode.
        /// </summary>
        public bool ReceiverKeepBinary { get; set; }

        /// <summary>
        /// Gets or sets a custom stream receiver.
        /// Stream receiver is invoked for every cache entry on the primary server node for that entry.
        /// </summary>
        public IStreamReceiver<TK, TV> Receiver { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether existing values can be overwritten by the data streamer.
        /// Performance is better when this flag is false.
        /// <para />
        /// NOTE: When false, cache updates won't be propagated to cache store
        /// (even if <see cref="SkipStore"/> is false).
        /// <para />
        /// Default is <c>false</c>.
        /// </summary>
        public bool AllowOverwrite { get; set; }

        /// <summary>
        /// Flag indicating that write-through behavior should be disabled for data loading.
        /// <para />
        /// <see cref="AllowOverwrite"/> must be true for write-through to work.
        /// <para />
        /// Default is <c>false</c>.
        /// </summary>
        public bool SkipStore { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether stream receiver should operate on data in binary mode.
        /// </summary>
        public bool KeepBinary { get; set; }

        /// <summary>
        /// Size of per node key-value pairs buffer.
        /// <para />
        /// Default is <see cref="DataStreamerDefaults.DefaultPerNodeBufferSize"/>.
        /// </summary>
        public int ClientPerNodeBufferSize { get; set; }

        /// <summary>
        /// Size of per node key-value pairs buffer.
        /// <para />
        /// Default is <see cref="DataStreamerDefaults.DefaultPerNodeBufferSize"/>.
        /// </summary>
        [DefaultValue(DataStreamerClientDefaults.ServerPerNodeBufferSize)]
        public int ServerPerNodeBufferSize { get; set; }

        /// <summary>
        /// Size of per thread key-value pairs buffer.
        /// <para />
        /// Default is <see cref="DataStreamerDefaults.DefaultPerThreadBufferSize"/>.
        /// </summary>
        [DefaultValue(DataStreamerClientDefaults.ServerPerThreadBufferSize)]
        public int ServerPerThreadBufferSize { get; set; }

        public int ClientPerNodeParallelOperations { get; set; }
    }
}
