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

namespace Apache.Ignite.Core.Cache.Query.Continuous
{
    using System;
    using Apache.Ignite.Core.Cache.Event;

    /// <summary>
    /// Continuous query base class, see <see cref="ContinuousQuery{K, V}"/>.
    /// </summary>
    public abstract class ContinuousQuery
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ContinuousQuery"/> class.
        /// </summary>
        protected internal ContinuousQuery()
        {
            // No-op.
        }

        /// <summary>
        /// Default buffer size.
        /// </summary>
        public const int DefaultBufferSize = 1;

        /// <summary>
        /// Default time interval.
        /// </summary>
        public static readonly TimeSpan DefaultTimeInterval = new TimeSpan(0);

        /// <summary>
        /// Default auto-unsubscribe flag value.
        /// </summary>
        public const bool DefaultAutoUnsubscribe = true;
    }

    /// <summary>
    /// API for configuring continuous cache queries.
    /// <para />
    /// Continuous queries allow to register a remote and a listener for cache update events.
    /// If an update event passes the filter, it will be sent to the node that executed the
    /// query and listener will be notified on that node.
    /// <para />
    /// Continuous query can either be executed on the whole topology or only on local node.
    /// <para />
    /// In case query is distributed and a new node joins, it will get the filter for the query
    /// during discovery process before it actually joins topology, so no updates will be missed.
    /// <para />
    /// To execute the query use method
    /// <see cref="ICache{K,V}.QueryContinuous(ContinuousQuery{K,V})"/>.
    /// </summary>
    public class ContinuousQuery<TK, TV> : ContinuousQuery
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="lsnr">Listener.</param>
        public ContinuousQuery(ICacheEntryEventListener<TK, TV> lsnr) : this(lsnr, false)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="lsnr">Listener.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        public ContinuousQuery(ICacheEntryEventListener<TK, TV> lsnr, bool loc) : this(lsnr, null, loc)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="lsnr">Listener.</param>
        /// <param name="filter">Filter.</param>
        public ContinuousQuery(ICacheEntryEventListener<TK, TV> lsnr, ICacheEntryEventFilter<TK, TV> filter)
            : this(lsnr, filter, false)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="lsnr">Listener.</param>
        /// <param name="filter">Filter.</param>
        /// <param name="loc">Whether query should be executed locally.</param>
        public ContinuousQuery(ICacheEntryEventListener<TK, TV> lsnr, ICacheEntryEventFilter<TK, TV> filter, bool loc)
        {
            Listener = lsnr;
            Filter = filter;
            Local = loc;

            BufferSize = DefaultBufferSize;
            TimeInterval = DefaultTimeInterval;
            AutoUnsubscribe = DefaultAutoUnsubscribe;
        }

        /// <summary>
        /// Cache entry event listener. Invoked on the node where continuous query execution
        /// has been started.
        /// </summary>
        public ICacheEntryEventListener<TK, TV> Listener { get; set; }

        /// <summary>
        /// Optional cache entry filter. Invoked on a node where cache event occurred. If filter
        /// returns <c>false</c>, then cache entry event will not be sent to a node where
        /// continuous query has been started.
        /// <para />
        /// Must be either binary or serializable in case query is not local.
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

        /// <summary>
        /// Automatic unsubscribe flag. This flag indicates that query filters on remote nodes
        /// should be automatically unregistered if master node (node that initiated the query)
        /// leaves topology. If this flag is <c>false</c>, filters will be unregistered only
        /// when the query is cancelled from master node, and won't ever be unregistered if
        /// master node leaves grid.
        /// <para />
        /// Defaults to <c>true</c>.
        /// </summary>
        public bool AutoUnsubscribe { get; set; }

        /// <summary>
        /// Local flag. When set query will be executed only on local node, so only local
        /// entries will be returned as query result.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool Local { get; set; }
        
        /// <summary>
        /// Gets or sets a value indicating whether to notify about <see cref="CacheEntryEventType.Expired"/> events.
        /// <para />
        /// If <c>true</c>, then the remote listener will get notifications about expired cache entries.
        /// Otherwise, only <see cref="CacheEntryEventType.Created"/>, <see cref="CacheEntryEventType.Updated"/>, and
        /// <see cref="CacheEntryEventType.Removed"/> events will be passed to the listener.
        /// <para />
        /// Defaults to <c>false</c>.
        /// </summary>
        public bool IncludeExpired { get; set; }

        /// <summary>
        /// Validate continuous query state.
        /// </summary>
        internal void Validate()
        {
            if (Listener == null)
                throw new ArgumentException("Listener cannot be null.");
        }
    }
}
