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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service context.
    /// </summary>
    internal class ServiceContext : IServiceContext
    {
        /** Service call context of the current thread. */
        private static readonly AsyncLocal<IServiceCallContext> LocalCallContext = new AsyncLocal<IServiceCallContext>();

        /// <summary>
        /// Default constructor.
        /// </summary>
        internal ServiceContext()
        {
            // No-op.
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceContext"/> class.
        /// </summary>
        /// <param name="svc">Service.</param>
        /// <param name="interceptor">Service call interceptor.</param>
        /// <param name="reader">The reader.</param>
        public ServiceContext(IService svc, IServiceCallInterceptor interceptor, IBinaryRawReader reader)
        {
            Service = svc;
            Interceptor = interceptor;

            if (reader == null)
                return;
                
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
        public bool IsCancelled { get; set; }

        /** <inheritdoc /> */
        public string CacheName { get; private set; }

        /** <inheritdoc /> */
        public object AffinityKey { get; private set; }

        /** <inheritdoc /> */
        public IServiceCallContext CurrentCallContext
        {
            get { return LocalCallContext.Value; }
        }

        /// <summary>
        /// 
        /// </summary>
        internal IService Service { get; set; }
        
        /// <summary>
        /// 
        /// </summary>
        internal IServiceCallInterceptor Interceptor { get; }

        /// <summary>
        /// Sets service call context for the current thread.
        /// </summary>
        /// <param name="callCtx">Service call context for the current thread.</param>
        internal static void SetCurrentCallContext(IServiceCallContext callCtx) {
            LocalCallContext.Value = callCtx;
        }
    }
}
