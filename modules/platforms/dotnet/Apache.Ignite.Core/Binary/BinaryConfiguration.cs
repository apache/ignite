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
        /// Default <see cref="DefaultKeepDeserialized"/> setting.
        /// </summary>
        public const bool DefaultDefaultKeepDeserialized = true;

        /** Footer setting. */
        private bool? _compactFooter;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration"/> class.
        /// </summary>
        public BinaryConfiguration()
        {
            DefaultKeepDeserialized = DefaultDefaultKeepDeserialized;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryConfiguration" /> class.
        /// </summary>
        /// <param name="cfg">The binary configuration to copy.</param>
        public BinaryConfiguration(BinaryConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "cfg");

            DefaultIdMapper = cfg.DefaultIdMapper;
            DefaultNameMapper = cfg.DefaultNameMapper;
            DefaultKeepDeserialized = cfg.DefaultKeepDeserialized;
            DefaultSerializer = cfg.DefaultSerializer;

            TypeConfigurations = cfg.TypeConfigurations == null
                ? null
                : cfg.TypeConfigurations.Select(x => new BinaryTypeConfiguration(x)).ToList();

            Types = cfg.Types == null ? null : cfg.Types.ToList();

            CompactFooter = cfg.CompactFooter;
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
        public IBinaryNameMapper DefaultNameMapper { get; set; }

        /// <summary>
        /// Default ID mapper.
        /// </summary>
        public IBinaryIdMapper DefaultIdMapper { get; set; }

        /// <summary>
        /// Default serializer.
        /// </summary>
        public IBinarySerializer DefaultSerializer { get; set; }

        /// <summary>
        /// Default keep deserialized flag.
        /// </summary>
        [DefaultValue(DefaultDefaultKeepDeserialized)]
        public bool DefaultKeepDeserialized { get; set; }

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
        /// Gets the compact footer internal nullable value.
        /// </summary>
        internal bool? CompactFooterInternal
        {
            get { return _compactFooter; }
        }

        /// <summary>
        /// Merges other config into this.
        /// </summary>
        internal void MergeTypes(BinaryConfiguration localConfig)
        {
            if (TypeConfigurations == null)
            {
                TypeConfigurations = localConfig.TypeConfigurations;
            }
            else if (localConfig.TypeConfigurations != null)
            {
                // Both configs are present.
                // Local configuration is more complete and takes preference when it exists for a given type.
                var localTypeNames = new HashSet<string>(localConfig.TypeConfigurations.Select(x => x.TypeName), 
                    StringComparer.OrdinalIgnoreCase);

                var configs = new List<BinaryTypeConfiguration>(localConfig.TypeConfigurations);

                configs.AddRange(TypeConfigurations.Where(x=>!localTypeNames.Contains(x.TypeName)));

                TypeConfigurations = configs;
            }
        }
    }
}
