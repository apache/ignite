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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Configuration defining various aspects of cache keys without explicit usage of annotations on user classes.
    /// </summary>
    public sealed class CacheKeyConfiguration : IBinaryRawWriteAware
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheKeyConfiguration"/> class.
        /// </summary>
        public CacheKeyConfiguration()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheKeyConfiguration"/> class, using specified type to look
        /// for <see cref="AffinityKeyMappedAttribute"/>.
        /// </summary>
        public CacheKeyConfiguration(Type keyType)
        {
            IgniteArgumentCheck.NotNull(keyType, "keyType");

            TypeName = keyType.FullName;
            AffinityKeyFieldName = AffinityKeyMappedAttribute.GetFieldNameFromAttribute(keyType);
        }

        /// <summary>
        /// Gets or sets the name of the key type.
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Gets or sets the name of the affinity key field.
        /// See also <see cref="AffinityKeyMappedAttribute"/>, 
        /// <see cref="BinaryTypeConfiguration.AffinityKeyFieldName"/>.
        /// </summary>
        public string AffinityKeyFieldName { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheKeyConfiguration"/> class.
        /// </summary>
        internal CacheKeyConfiguration(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            TypeName = reader.ReadString();
            AffinityKeyFieldName = reader.ReadString();
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        void IBinaryRawWriteAware<IBinaryRawWriter>.Write(IBinaryRawWriter writer)
        {
            writer.WriteString(TypeName);
            writer.WriteString(AffinityKeyFieldName);
        }
    }
}