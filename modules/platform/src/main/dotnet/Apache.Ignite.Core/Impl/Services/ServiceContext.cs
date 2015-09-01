/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service context.
    /// </summary>
    internal class ServiceContext : IServiceContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceContext"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ServiceContext(IPortableRawReader reader)
        {
            Debug.Assert(reader != null);

            Name = reader.ReadString();
            ExecutionId = reader.ReadGuid() ?? Guid.Empty;
            IsCancelled = reader.ReadBoolean();
            CacheName = reader.ReadString();
            AffinityKey = reader.ReadObject<object>();
        }

        /** <inheritdoc /> */
        public string Name { get; private set; }

        /** <inheritdoc /> */
        public Guid ExecutionId { get; private set; }

        /** <inheritdoc /> */
        public bool IsCancelled { get; private set; }

        /** <inheritdoc /> */
        public string CacheName { get; private set; }

        /** <inheritdoc /> */
        public object AffinityKey { get; private set; }
    }
}