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
    using System.Collections;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service proxy context.
    /// </summary>
    internal class ServiceProxyContextImpl : ServiceProxyContext
    {
        /** Context attributes. */
        private readonly Hashtable _attrs;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="attrs">Context attributes.</param>
        internal ServiceProxyContextImpl(Hashtable attrs)
        {
            IgniteArgumentCheck.NotNull(attrs, "attrs");
            IgniteArgumentCheck.Ensure(attrs.Count > 0, "attrs", "cannot create an empty context.");

            _attrs = attrs;
        }

        /// <summary>
        /// Constructor to create an empty context.
        /// </summary>
        internal ServiceProxyContextImpl()
        {
            _attrs = new Hashtable(0);
        }
        
        /** <inheritDoc /> */
        public override object Attribute(string name)
        {
            return _attrs[name];
        }

        /// <summary>
        /// Set the service proxy context for the current thread.
        /// </summary>
        /// <param name="attrs">Context attributes.</param>
        internal static void Current(Hashtable attrs)
        {
            ProxyCtxs.Value = attrs;
        }

        /// <summary>
        /// Get context attributes.
        /// </summary>
        /// <returns>Context attributes.</returns>
        internal Hashtable Values()
        {
            return _attrs;
        }
    }
}