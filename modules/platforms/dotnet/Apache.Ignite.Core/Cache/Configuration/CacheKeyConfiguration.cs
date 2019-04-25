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