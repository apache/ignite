/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Portable
{
    using System;
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

            Types = cfg.Types != null ? new List<String>(cfg.Types) : null;

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
        public ICollection<String> Types
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
