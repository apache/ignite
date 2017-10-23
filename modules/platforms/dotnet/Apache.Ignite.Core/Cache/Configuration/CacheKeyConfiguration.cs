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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Configuration defining various aspects of cache keys without explicit usage of annotations on user classes.
    /// </summary>
    public sealed class CacheKeyConfiguration
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheKeyConfiguration"/> class.
        /// </summary>
        public CacheKeyConfiguration()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheKeyConfiguration"/> class.
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
    }
}