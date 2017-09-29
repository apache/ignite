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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary type configuration.
    /// </summary>
    public class BinaryConfiguration
    {
        /// <summary>
        /// Default <see cref="CompactFooter"/> setting.
        /// </summary>
        public const bool DefaultCompactFooter = true;

        /// <summary>
        /// Default <see cref="KeepDeserialized"/> setting.
        /// </summary>
        public const bool DefaultKeepDeserialized = true;

        /// <summary>
        /// Default <see cref="UseVarintArrayLength"/> setting.
        /// </summary>
        public const bool DefaultUseVarintArrayLength = false;

        /** Footer setting. */
        private bool? _compactFooter;

        /** Varint array length setting. */
        private bool? _useVarintArrayLength;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration"/> class.
        /// </summary>
        public BinaryConfiguration()
        {
            KeepDeserialized = DefaultKeepDeserialized;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration" /> class.
        /// </summary>
        /// <param name="cfg">The binary configuration to copy.</param>
        public BinaryConfiguration(BinaryConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "cfg");

            CopyLocalProperties(cfg);
        }

        /// <summary>
        /// Copies the local properties.
        /// </summary>
        internal void CopyLocalProperties(BinaryConfiguration cfg)
        {
            Debug.Assert(cfg != null);

            IdMapper = cfg.IdMapper;
            NameMapper = cfg.NameMapper;
            KeepDeserialized = cfg.KeepDeserialized;

            if (cfg.Serializer != null)
            {
                Serializer = cfg.Serializer;
            }

            TypeConfigurations = cfg.TypeConfigurations == null
                ? null
                : cfg.TypeConfigurations.Select(x => new BinaryTypeConfiguration(x)).ToList();

            Types = cfg.Types == null ? null : cfg.Types.ToList();

            if (cfg.CompactFooterInternal != null)
            {
                CompactFooter = cfg.CompactFooterInternal.Value;
            }
          
            UseVarintArrayLength = cfg.UseVarintArrayLength;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration"/> class.
        /// </summary>
        /// <param name="binaryTypes">Binary types to register.</param>
        public BinaryConfiguration(params Type[] binaryTypes)
        {
            TypeConfigurations = binaryTypes.Select(t => new BinaryTypeConfiguration(t)).ToList();
        }

        /// <summary>
        /// Type configurations.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<BinaryTypeConfiguration> TypeConfigurations { get; set; }

        /// <summary>
        /// Gets or sets a collection of assembly-qualified type names 
        /// (the result of <see cref="Type.AssemblyQualifiedName"/>) for binarizable types.
        /// <para />
        /// Shorthand for creating <see cref="BinaryTypeConfiguration"/>.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<string> Types { get; set; }

        /// <summary>
        /// Default name mapper.
        /// </summary>
        public IBinaryNameMapper NameMapper { get; set; }

        /// <summary>
        /// Default ID mapper.
        /// </summary>
        public IBinaryIdMapper IdMapper { get; set; }

        /// <summary>
        /// Default serializer.
        /// </summary>
        public IBinarySerializer Serializer { get; set; }

        /// <summary>
        /// Default keep deserialized flag.
        /// </summary>
        [DefaultValue(DefaultKeepDeserialized)]
        public bool KeepDeserialized { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to write footers in compact form.
        /// When enabled, Ignite will not write fields metadata when serializing objects, 
        /// because internally metadata is distributed inside cluster.
        /// This increases serialization performance.
        /// <para/>
        /// <b>WARNING!</b> This mode should be disabled when already serialized data can be taken from some external
        /// sources (e.g.cache store which stores data in binary form, data center replication, etc.). 
        /// Otherwise binary objects without any associated metadata could could not be deserialized.
        /// </summary>
        [DefaultValue(DefaultCompactFooter)]
        public bool CompactFooter
        {
            get { return _compactFooter ?? DefaultCompactFooter; }
            set { _compactFooter = value; }
        }

        /// <summary>
        /// Indicates whether to consider arrays lengths in varint encoding. When enabled, Ignite will consider arrays
        /// lengths in varint encoding.
        /// <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">Varint encoding description.
        /// </a>
        /// <para/>
        /// <b>WARNING!</b> This mode should be disabled when already serialized data can be taken from some external
        /// sources (e.g.cache store which stores data in binary form, data center replication, etc.). 
        /// Otherwise binary objects without any associated metadata could could not be deserialized.
        /// </summary>
        [DefaultValue(DefaultUseVarintArrayLength)]
        public bool UseVarintArrayLength
        {
            get { return _useVarintArrayLength ?? DefaultUseVarintArrayLength; }
            set { _useVarintArrayLength = value; }
        }

        /// <summary>
        /// Gets the compact footer internal nullable value.
        /// </summary>
        internal bool? CompactFooterInternal
        {
            get { return _compactFooter; }
        }
    }
}
