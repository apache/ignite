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

namespace Apache.Ignite.Core.Services
{
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Services;

    /// <summary>
    /// Service operation context.
    /// todo extended doc with examples
    /// </summary>
    public abstract class ServiceProxyContext
    {
        /** Global mapping of the thread ID to the service proxy context. */
        protected static readonly ConcurrentDictionary<int, Hashtable> ProxyCtxs = 
            new ConcurrentDictionary<int, Hashtable>();

        /** Empty context. */
        private static readonly ServiceProxyContext EmptyCtx = new ServiceProxyContextImpl();

        /// <summary>
        /// Get the value of the proxy context attribute.
        /// </summary>
        /// <param name="name">Context attribute name.</param>
        /// <returns>Context attribute value.</returns>
        public abstract object Attribute(string name);

        /// <summary>
        /// Get the service proxy context of the current thread.
        /// </summary>
        /// <returns>Service proxy context.</returns> 
        public static ServiceProxyContext Current()
        {
            Hashtable attrs;

            if (ProxyCtxs.TryGetValue(Thread.CurrentThread.ManagedThreadId, out attrs))
                return new ServiceProxyContextImpl(attrs);

            // Return an empty context.
            return EmptyCtx;
        }
    }
}