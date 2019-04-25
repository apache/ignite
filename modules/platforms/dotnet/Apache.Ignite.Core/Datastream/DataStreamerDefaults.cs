/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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

    /// <summary>
    /// Data streamer configuration defaults.
    /// </summary>
    public static class DataStreamerDefaults
    {
        /// <summary>
        /// The default per node buffer size, see <see cref="IDataStreamer{TK,TV}.PerNodeBufferSize"/>.
        /// </summary>
        public const int DefaultPerNodeBufferSize = 512;
        
        /// <summary>
        /// The default per thread buffer size, see <see cref="IDataStreamer{TK,TV}.PerThreadBufferSize"/>.
        /// </summary>
        public const int DefaultPerThreadBufferSize = 4096;

        /// <summary>
        /// Default multiplier for parallel operations per node:
        /// <see cref="IDataStreamer{TK,TV}.PerNodeParallelOperations"/> = 
        /// <see cref="IgniteConfiguration.DataStreamerThreadPoolSize"/> * 
        /// <see cref="DefaultParallelOperationsMultiplier"/>.
        /// </summary>
        public const int DefaultParallelOperationsMultiplier = 8;

        /// <summary>
        /// The default timeout (see <see cref="IDataStreamer{TK,TV}.Timeout"/>).
        /// Negative value means no timeout.
        /// </summary>
        public static readonly TimeSpan DefaultTimeout = TimeSpan.FromMilliseconds(-1);
    }
}
