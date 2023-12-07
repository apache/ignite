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
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Thin client data streamer options.
    /// <para />
    /// To set a receiver, use generic class <see cref="DataStreamerClientOptions{K,V}"/>.
    /// <para />
    /// See also <see cref="IDataStreamerClient{TK,TV}"/>, <see cref="IIgniteClient.GetDataStreamer{TK,TV}(string)"/>.
    /// </summary>
    public class DataStreamerClientOptions
    {
        /// <summary>
        /// The default client-side per-node buffer size (cache entries count),
        /// see <see cref="DataStreamerClientOptions.PerNodeBufferSize"/>.
        /// </summary>
        public const int DefaultPerNodeBufferSize = 512;

        /// <summary>
        /// The default limit for parallel operations per server node connection,
        /// see <see cref="DataStreamerClientOptions.PerNodeParallelOperations"/>.
        /// <para />
        /// Calculated as <see cref="Environment.ProcessorCount"/> times 4.
        /// </summary>
        public static readonly int DefaultPerNodeParallelOperations = Environment.ProcessorCount * 4;

        /** */
        private int _perNodeParallelOperations;

        /** */
        private int _perNodeBufferSize;

        /** */
        private TimeSpan _autoFlushInterval;

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions"/>.
        /// </summary>
        public DataStreamerClientOptions()
        {
            PerNodeBufferSize = DefaultPerNodeBufferSize;
            PerNodeParallelOperations = DefaultPerNodeParallelOperations;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions"/>.
        /// </summary>
        /// <param name="options">Options to copy from.</param>
        public DataStreamerClientOptions(DataStreamerClientOptions options) : this()
        {
            if (options == null)
            {
                return;
            }

            ReceiverKeepBinary = options.ReceiverKeepBinary;
            ReceiverInternal = options.ReceiverInternal;
            AllowOverwrite = options.AllowOverwrite;
            SkipStore = options.SkipStore;
            PerNodeBufferSize = options.PerNodeBufferSize;
            PerNodeParallelOperations = options.PerNodeParallelOperations;
            AutoFlushInterval = options.AutoFlushInterval;
        }

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="DataStreamerClientOptions{K,V}.Receiver"/>
        /// should operate in binary mode.
        /// </summary>
        public bool ReceiverKeepBinary { get; set; }

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
        /// Gets or sets a flag indicating that write-through behavior should be disabled for data loading.
        /// <para />
        /// <see cref="AllowOverwrite"/> must be true for write-through to work.
        /// <para />
        /// Default is <c>false</c>.
        /// </summary>
        public bool SkipStore { get; set; }

        /// <summary>
        /// Gets or sets the size (entry count) of per node buffer.
        /// <para />
        /// Default is <see cref="DefaultPerNodeBufferSize"/>.
        /// </summary>
        public int PerNodeBufferSize
        {
            get { return _perNodeBufferSize; }
            set
            {
                IgniteArgumentCheck.Ensure(value > 0, "value", "should be > 0");
                _perNodeBufferSize = value;
            }
        }

        /// <summary>
        /// Gets or sets the limit for parallel operations per server node.
        /// <para />
        /// Default is <see cref="DefaultPerNodeParallelOperations"/>.
        /// </summary>
        public int PerNodeParallelOperations
        {
            get { return _perNodeParallelOperations; }
            set
            {
                IgniteArgumentCheck.Ensure(value > 0, "value", "should be > 0");
                _perNodeParallelOperations = value;
            }
        }

        /// <summary>
        /// Gets or sets the automatic flush interval. Data streamer buffers the data for performance reasons.
        /// The buffer is flushed in the following cases:
        /// <ul>
        /// <li>Buffer is full.</li>
        /// <li><see cref="IDataStreamerClient{TK,TV}.Flush"/> is called.</li>
        /// <li>Periodically when <see cref="AutoFlushInterval"/> is set.</li >
        /// </ul>
        /// <para />
        /// When set to <see cref="TimeSpan.Zero"/>, automatic flush is disabled.
        /// <para />
        /// Default is <see cref="TimeSpan.Zero"/> (disabled).
        /// </summary>
        public TimeSpan AutoFlushInterval
        {
            get { return _autoFlushInterval; }
            set
            {
                IgniteArgumentCheck.Ensure(value >= TimeSpan.Zero, "value", "should be >= 0");
                _autoFlushInterval = value;
            }
        }

        /// <summary>
        /// Gets or sets the receiver object.
        /// </summary>
        internal object ReceiverInternal { get; set; }
    }

    /// <summary>
    /// Thin client data streamer extended options.
    /// <para />
    /// See also <see cref="IDataStreamerClient{TK,TV}"/>, <see cref="IIgniteClient.GetDataStreamer{TK,TV}(string)"/>.
    /// </summary>
    public class DataStreamerClientOptions<TK, TV> : DataStreamerClientOptions
    {
        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions{TK,TV}"/>.
        /// </summary>
        public DataStreamerClientOptions()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions{TK,TV}"/>.
        /// </summary>
        /// <param name="options">Options to copy from.</param>
        public DataStreamerClientOptions(DataStreamerClientOptions options) : base(options)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of <see cref="DataStreamerClientOptions{TK,TV}"/>.
        /// </summary>
        /// <param name="options">Options to copy from.</param>
        public DataStreamerClientOptions(DataStreamerClientOptions<TK, TV> options) : base(options)
        {
            // No-op.
        }

        /// <summary>
        /// Gets or sets a custom stream receiver.
        /// Stream receiver is invoked for every cache entry on the primary server node for that entry.
        /// </summary>
        public IStreamReceiver<TK, TV> Receiver
        {
            get { return (IStreamReceiver<TK, TV>) ReceiverInternal; }
            set { ReceiverInternal = value; }
        }
    }
}
