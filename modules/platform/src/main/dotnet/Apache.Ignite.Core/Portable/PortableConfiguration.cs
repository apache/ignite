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

namespace Apache.Ignite.Core.Portable
{
    using System.Collections.Generic;

    /// <summary>
    /// Portable type configuration.
    /// </summary>
    public class PortableConfiguration
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public PortableConfiguration()
        {
            DefaultMetadataEnabled = true;
            DefaultKeepDeserialized = true;
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration to copy.</param>
        public PortableConfiguration(PortableConfiguration cfg)
        {
            DefaultIdMapper = cfg.DefaultIdMapper;
            DefaultNameMapper = cfg.DefaultNameMapper;
            DefaultMetadataEnabled = cfg.DefaultMetadataEnabled;
            DefaultKeepDeserialized = cfg.DefaultKeepDeserialized;
            DefaultSerializer = cfg.DefaultSerializer;

            Types = cfg.Types != null ? new List<string>(cfg.Types) : null;

            if (cfg.TypeConfigurations != null)
            {
                TypeConfigurations = new List<PortableTypeConfiguration>(cfg.TypeConfigurations.Count);

                foreach (PortableTypeConfiguration typeCfg in cfg.TypeConfigurations) 
                    TypeConfigurations.Add(new PortableTypeConfiguration(typeCfg));
            }
        }

        /// <summary>
        /// Type configurations.
        /// </summary>
        public ICollection<PortableTypeConfiguration> TypeConfigurations
        {
            get;
            set;
        }

        /// <summary>
        /// Portable types. Shorthand for creating PortableTypeConfiguration.
        /// </summary>
        public ICollection<string> Types
        {
            get;
            set;
        }

        /// <summary>
        /// Default name mapper.
        /// </summary>
        public IPortableNameMapper DefaultNameMapper
        {
            get;
            set;
        }

        /// <summary>
        /// Default ID mapper.
        /// </summary>
        public IPortableIdMapper DefaultIdMapper
        {
            get;
            set;
        }

        /// <summary>
        /// Default serializer.
        /// </summary>
        public IPortableSerializer DefaultSerializer
        {
            get;
            set;
        }

        /// <summary>
        /// Default metadata enabled flag. Defaults to true.
        /// </summary>
        public bool DefaultMetadataEnabled
        {
            get;
            set;
        }

        /// <summary>
        /// Default keep deserialized flag.
        /// </summary>
        public bool DefaultKeepDeserialized
        {
            get;
            set;
        }
    }
}
