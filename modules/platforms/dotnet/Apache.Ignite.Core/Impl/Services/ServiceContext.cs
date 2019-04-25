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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
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
        public ServiceContext(IBinaryRawReader reader)
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