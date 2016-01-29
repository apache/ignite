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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary type configuration.
    /// </summary>
    public class BinaryConfiguration
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public BinaryConfiguration()
        {
            DefaultKeepDeserialized = true;
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration to copy.</param>
        public BinaryConfiguration(BinaryConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "cfg");

            DefaultIdMapper = cfg.DefaultIdMapper;
            DefaultNameMapper = cfg.DefaultNameMapper;
            DefaultKeepDeserialized = cfg.DefaultKeepDeserialized;
            DefaultSerializer = cfg.DefaultSerializer;

            Types = cfg.Types != null ? new List<string>(cfg.Types) : null;

            if (cfg.TypeConfigurations != null)
            {
                TypeConfigurations = new List<BinaryTypeConfiguration>(cfg.TypeConfigurations.Count);

                foreach (BinaryTypeConfiguration typeCfg in cfg.TypeConfigurations)
                    TypeConfigurations.Add(new BinaryTypeConfiguration(typeCfg));
            }
        }

        /// <summary>
        /// Type configurations.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<BinaryTypeConfiguration> TypeConfigurations { get; set; }

        /// <summary>
        /// Binarizable types. Shorthand for creating <see cref="BinaryTypeConfiguration"/>.
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
        public bool DefaultKeepDeserialized { get; set; }
    }
}
