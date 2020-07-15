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
namespace Apache.Ignite.Core.Client.Cache.Query.Continuous
{
    using System;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Interop;

    /// <summary>
    /// API for configuring continuous cache queries in thin client.
    /// <para />
    /// Continuous queries allow to register a remote filter and a listener for cache update events.
    /// If an update event passes the filter, it will be sent to the client.
    /// <para />
    /// To execute the query use method
    /// <see cref="ICacheClient{K,V}.QueryContinuous"/>.
    /// </summary>
    public class ContinuousQueryClient<TK, TV>
    {
        /// <summary>
        /// Default buffer size.
        /// </summary>
        public const int DefaultBufferSize = 1;

        /// <summary>
        /// Initializes a new instance of <see cref="ContinuousQueryClient{TK,TV}"/> class.
        /// </summary>
        public ContinuousQueryClient()
        {
            BufferSize = DefaultBufferSize;
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ContinuousQueryClient{TK,TV}"/> class with a specified listener.
        /// </summary>
        public ContinuousQueryClient(ICacheEntryEventListener<TK, TV> listener) : this()
        {
            IgniteArgumentCheck.NotNull(listener, "listener");

            Listener = listener;
        }

        /// <summary>
        /// Cache entry event listener. Invoked locally.
        /// </summary>
        public ICacheEntryEventListener<TK, TV> Listener { get; set; }

        /// <summary>
        /// Optional cache entry filter. Invoked on a node where cache event occurred. If the filter
        /// returns <c>false</c>, then cache entry event will not be sent to a node where the
        /// continuous query has been started.
        /// <para />
        /// This filter will be serialized and sent to the server nodes. .NET filters require all
        /// server nodes to be .NET-based. Java filters can be used with <see cref="JavaObject"/>
        /// and <see cref="ContinuousQueryExtensions.ToCacheEntryEventFilter{K, V}"/>.
        /// </summary>
        public ICacheEntryEventFilter<TK, TV> Filter { get; set; }

        /// <summary>
        /// Buffer size. When a cache update happens, entry is first put into a buffer.
        /// Entries from buffer will be sent to the master node only if the buffer is
        /// full or time provided via <see cref="TimeInterval"/> is exceeded.
        /// <para />
        /// Defaults to <see cref="ContinuousQuery.DefaultBufferSize"/>
        /// </summary>
        public int BufferSize { get; set; }

        /// <summary>
        /// Time interval. When a cache update happens, entry is first put into a buffer.
        /// Entries from buffer will be sent to the master node only if the buffer is full
        /// (its size can be provided via <see cref="BufferSize"/> property) or time provided
        /// via this method is exceeded.
        /// <para />
        /// Defaults to <c>0</c> which means that time check is disabled and entries will be
        /// sent only when buffer is full.
        /// </summary>
        public TimeSpan TimeInterval { get; set; }
    }
}
