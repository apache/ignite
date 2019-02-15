/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Maps to PlatformJavaObjectFactoryProxy in Java.
    /// </summary>
    internal class PlatformJavaObjectFactoryProxy : IBinaryWriteAware
    {
        /// <summary>
        /// Represents the factory type.
        /// </summary>
        internal enum FactoryType
        {
            User = 0,
            Default = 1
        }

        /** Type code. */
        private readonly FactoryType _factoryType;

        /** Java class name */
        private readonly string _factoryClassName;

        /** Optional payload. */
        private readonly object _payload;

        /** Properties to set */
        private readonly IDictionary<string, object> _properties;

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformJavaObjectFactoryProxy" /> class.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="factoryClassName">Name of the factory class.</param>
        /// <param name="payload">The payload.</param>
        /// <param name="properties">The properties.</param>
        protected PlatformJavaObjectFactoryProxy(FactoryType type, string factoryClassName, object payload, 
            IDictionary<string, object> properties)
        {
            _factoryType = type;
            _factoryClassName = factoryClassName;
            _payload = payload;
            _properties = properties;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PlatformJavaObjectFactoryProxy"/> class.
        /// </summary>
        public PlatformJavaObjectFactoryProxy()
        {
            throw new InvalidOperationException(GetType() + " should never be deserialized on .NET side.");
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var w = writer.GetRawWriter();

            w.WriteInt((int) _factoryType);
            w.WriteString(_factoryClassName);
            w.WriteObject(_payload);

            if (_properties != null)
            {
                w.WriteInt(_properties.Count);

                foreach (var pair in _properties)
                {
                    w.WriteString(pair.Key);
                    w.WriteObject(pair.Value);
                }
            }
            else
                w.WriteInt(0);
        }

        /// <summary>
        /// Gets the raw proxy (not the derived type) for serialization.
        /// </summary>
        public PlatformJavaObjectFactoryProxy GetRawProxy()
        {
            return new PlatformJavaObjectFactoryProxy(_factoryType, _factoryClassName, _payload, _properties);
        }
    }
}
