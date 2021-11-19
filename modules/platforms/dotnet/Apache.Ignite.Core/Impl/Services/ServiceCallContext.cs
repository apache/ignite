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
    using System.Collections.Generic;
    using System.Text;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service call context implementation.
    /// </summary>
    internal class ServiceCallContext : IServiceCallContext
    {
        /** Context attributes. */
        private Dictionary<string, byte[]> _attrs;

        /// <summary>
        /// Constructs context from dictionary.
        /// </summary>
        /// <param name="attrs">Context attributes.</param>
        internal ServiceCallContext(Dictionary<string, byte[]> attrs)
        {
            IgniteArgumentCheck.NotNull(attrs, "attrs");

            _attrs = attrs;
        }

        /** <inheritDoc /> */
        public string GetAttribute(string name)
        {
            byte[] bytes = GetBinaryAttribute(name);

            if (bytes != null)
                return Encoding.UTF8.GetString(bytes);

            return null;
        }

        /** <inheritDoc /> */
        public byte[] GetBinaryAttribute(string name)
        {
            byte[] bytes;

            if (_attrs.TryGetValue(name, out bytes))
                return bytes;

            return null;
        }
        
        /// <summary>
        /// Gets call context attributes.
        /// </summary>
        /// <returns>Service call context attributes.</returns>
        internal Dictionary<string, byte[]> Values()
        {
            return _attrs;
        }
    }
}