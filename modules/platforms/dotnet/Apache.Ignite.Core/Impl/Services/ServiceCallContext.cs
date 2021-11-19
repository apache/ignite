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
    /// Service call context implementation.
    /// </summary>
    internal class ServiceCallContext : IServiceCallContext
    {
        /** Context attributes. */
        private IDictionary _attrs;

        /// <summary>
        /// Constructs context from dictionary.
        /// </summary>
        /// <param name="attrs">Context attributes.</param>
        internal ServiceCallContext(IDictionary attrs)
        {
            IgniteArgumentCheck.NotNull(attrs, "attrs");

            _attrs = attrs;
        }

        /** <inheritDoc /> */
        public string GetAttribute(string name)
        {
            return (string) _attrs[name];
        }

        /** <inheritDoc /> */
        public byte[] GetBinaryAttribute(string name)
        {
            return (byte[]) _attrs[name];
        }
        
        /// <summary>
        /// Gets call context attributes.
        /// </summary>
        /// <returns>Service call context attributes.</returns>
        internal IDictionary Values()
        {
            return _attrs;
        }
    }
}