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
    using System.Text;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service call context.
    /// </summary>
    internal class ServiceCallContextImpl : ServiceCallContext
    {
        /** Context attributes. */
        private readonly IDictionary _attrs;

        /** <inheritDoc /> */
        public override string Attribute(string name)
        {
            // Prevent throwing an exception when using a read-only dictionary.
            if (_attrs.Count == 0)
                return null;

            byte[] bytes = (byte[]) _attrs[name];

            if (bytes == null)
                return null;
            
            return Encoding.UTF8.GetString(bytes);
        }

        /** <inheritDoc /> */
        public override byte[] BinaryAttribute(string name)
        {
            return (byte[])_attrs[name];
        }

        /** <inheritDoc /> */
        public override ServiceCallContext Put(string name, string value)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(value, "value");

            _attrs[name] = Encoding.UTF8.GetBytes(value);

            return this;
        }

        /** <inheritDoc /> */
        public override ServiceCallContext Put(string name, byte[] value)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(value, "value");

            _attrs[name] = value;
            
            return this;
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        internal ServiceCallContextImpl() : this(new Hashtable()) { }

        /// <summary>
        /// Constructs context from dictionary.
        /// </summary>
        /// <param name="attrs">Context attributes.</param>
        internal ServiceCallContextImpl(IDictionary attrs)
        {
            IgniteArgumentCheck.NotNull(attrs, "attrs");

            _attrs = attrs;
        }

        /// <summary>
        /// Get context attributes.
        /// </summary>
        /// <returns>Context attributes.</returns>
        internal IDictionary Values()
        {
            return _attrs;
        }
    }
}