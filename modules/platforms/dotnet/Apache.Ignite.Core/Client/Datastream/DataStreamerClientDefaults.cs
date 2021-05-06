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

    /// <summary>
    /// Data streamer configuration defaults.
    /// </summary>
    public static class DataStreamerClientDefaults
    {
        /// <summary>
        /// The default per node buffer size,
        /// see <see cref="DataStreamerClientOptions{TK,TV}.ServerPerNodeBufferSize"/>.
        /// </summary>
        public const int ServerPerNodeBufferSize = 512;

        /// <summary>
        /// The default per node buffer size,
        /// see <see cref="DataStreamerClientOptions{TK,TV}.ClientPerNodeBufferSize"/>.
        /// </summary>
        public const int ClientPerNodeBufferSize = 512;

        /// <summary>
        /// The default per thread buffer size,
        /// see <see cref="DataStreamerClientOptions{TK,TV}.ServerPerThreadBufferSize"/>.
        /// </summary>
        public const int ServerPerThreadBufferSize = 4096;

        /// <summary>
        /// Default limit for parallel operations per server node,
        /// see <see cref="DataStreamerClientOptions{TK,TV}.ClientPerNodeParallelOperations"/>.
        /// </summary>
        public static readonly int ClientPerNodeParallelOperations = Environment.ProcessorCount;
    }
}
