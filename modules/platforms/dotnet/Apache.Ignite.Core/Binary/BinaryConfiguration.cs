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

        /** Footer setting. */
        private bool? _compactFooter;

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
        /// Gets the compact footer internal nullable value.
        /// </summary>
        internal bool? CompactFooterInternal
        {
            get { return _compactFooter; }
        }
    }
}
