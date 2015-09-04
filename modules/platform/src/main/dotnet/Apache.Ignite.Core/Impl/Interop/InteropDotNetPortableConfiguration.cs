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

namespace Apache.Ignite.Core.Impl.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// .Net portable configuration as defined in Java configuration.
    /// </summary>
    internal class InteropDotNetPortableConfiguration : IPortableWriteAware
    {
        /// <summary>
        /// Type configurations.
        /// </summary>
        public ICollection<InteropDotNetPortableTypeConfiguration> TypeConfigurations { get; set; }

        /// <summary>
        /// Portable types. Shorthand for creating PortableTypeConfiguration.
        /// </summary>
        public ICollection<string> Types { get; set; }

        /// <summary>
        /// Default name mapper.
        /// </summary>
        public string DefaultNameMapper { get; set; }

        /// <summary>
        /// Default ID mapper.
        /// </summary>
        public string DefaultIdMapper { get; set; }

        /// <summary>
        /// Default serializer.
        /// </summary>
        public string DefaultSerializer { get; set; }

        /// <summary>
        /// Default metadata enabled flag. Defaults to true.
        /// </summary>
        public bool DefaultMetadataEnabled { get; set; }

        /// <summary>
        /// Keep deserialized flag. If set to non-null value, overrides default value set in 
        /// PortableConfiguration.
        /// </summary>
        public bool DefaultKeepDeserialized { get; set; }

        /// <summary>
        /// Creates PortableConfiguration.
        /// </summary>
        /// <returns>PortableConfiguration</returns>
        public PortableConfiguration ToPortableConfiguration()
        {
            PortableConfiguration res = new PortableConfiguration();

            if (TypeConfigurations != null)
            {
                List<PortableTypeConfiguration> typeCfgs = new List<PortableTypeConfiguration>();

                foreach (InteropDotNetPortableTypeConfiguration dotNetTypeCfg in TypeConfigurations)
                    typeCfgs.Add(dotNetTypeCfg.ToPortableTypeConfiguration());

                res.TypeConfigurations = typeCfgs;
            }

            res.Types = Types;
            res.DefaultNameMapper =
                (IPortableNameMapper) InteropDotNetPortableTypeConfiguration.CreateInstance(DefaultNameMapper);
            res.DefaultIdMapper =
                (IPortableIdMapper) InteropDotNetPortableTypeConfiguration.CreateInstance(DefaultIdMapper);
            res.DefaultSerializer =
                (IPortableSerializer) InteropDotNetPortableTypeConfiguration.CreateInstance(DefaultSerializer);
            res.DefaultMetadataEnabled = DefaultMetadataEnabled;
            res.DefaultKeepDeserialized = DefaultKeepDeserialized;

            return res;
        }

        /** {@inheritDoc} */
        public void WritePortable(IPortableWriter writer)
        {
            IPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteGenericCollection(TypeConfigurations);
            rawWriter.WriteGenericCollection(Types);
            rawWriter.WriteString(DefaultNameMapper);
            rawWriter.WriteString(DefaultIdMapper);
            rawWriter.WriteString(DefaultSerializer);
            rawWriter.WriteBoolean(DefaultMetadataEnabled);
            rawWriter.WriteBoolean(DefaultKeepDeserialized);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InteropDotNetPortableConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public InteropDotNetPortableConfiguration(IPortableReader reader)
        {
            IPortableRawReader rawReader = reader.RawReader();

            TypeConfigurations = rawReader.ReadGenericCollection<InteropDotNetPortableTypeConfiguration>();
            Types = rawReader.ReadGenericCollection<string>();
            DefaultNameMapper = rawReader.ReadString();
            DefaultIdMapper = rawReader.ReadString();
            DefaultSerializer = rawReader.ReadString();
            DefaultMetadataEnabled = rawReader.ReadBoolean();
            DefaultKeepDeserialized = rawReader.ReadBoolean();
        }
    }
}
