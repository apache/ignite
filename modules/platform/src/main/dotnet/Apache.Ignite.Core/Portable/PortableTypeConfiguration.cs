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
    using System;

    /// <summary>
    /// Portable type configuration.
    /// </summary>
    public class PortableTypeConfiguration
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        public PortableTypeConfiguration()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeName">Type name.</param>
        public PortableTypeConfiguration(string typeName)
        {
            TypeName = typeName;
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param> 
        public PortableTypeConfiguration(Type type)
        {
            TypeName = type.FullName;
        }

        /// <summary>
        /// Copying constructor.
        /// </summary>
        /// <param name="cfg">Configuration to copy.</param>
        public PortableTypeConfiguration(PortableTypeConfiguration cfg)
        {
            AffinityKeyFieldName = cfg.AffinityKeyFieldName;
            AssemblyName = cfg.AssemblyName;
            IdMapper = cfg.IdMapper;
            NameMapper = cfg.NameMapper;
            Serializer = cfg.Serializer;
            TypeName = cfg.TypeName;
            MetadataEnabled = cfg.MetadataEnabled;
            KeepDeserialized = cfg.KeepDeserialized;
        }

        /// <summary>
        /// Assembly name. 
        /// </summary>
        public string AssemblyName
        {
            get;
            set;
        }

        /// <summary>
        /// Fully qualified type name. 
        /// </summary>
        public string TypeName
        {
            get;
            set;
        }
        
        /// <summary>
        /// Name mapper for the given type. 
        /// </summary>
        public IPortableNameMapper NameMapper
        {
            get;
            set;
        }

        /// <summary>
        /// ID mapper for the given type. When it is necessary to resolve class (field) ID, then 
        /// this property will be checked first. If not set, then PortableClassIdAttribute 
        /// (PortableFieldIdAttribute) will be checked in class through reflection. If required
        /// attribute is not set, then ID will be hash code of the class (field) simple name in lower case. 
        /// </summary>
        public IPortableIdMapper IdMapper
        {
            get;
            set;
        }

        /// <summary>
        /// Serializer for the given type. If not provided and class implements IPortable
        /// then its custom logic will be used. If not provided and class doesn't implement IPortable
        /// then all fields of the class except of those with [NotSerialized] attribute will be serialized
        ///with help of reflection.
        /// </summary>
        public IPortableSerializer Serializer
        {
            get;
            set;
        }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName
        {
            get;
            set;
        }

        /// <summary>
        /// Metadata enabled flag. If set to non-null value, overrides default value set in 
        /// PortableConfiguration.
        /// </summary>
        public bool? MetadataEnabled
        {
            get;
            set;
        }

        /// <summary>
        /// Keep deserialized flag. If set to non-null value, overrides default value set in 
        /// PortableConfiguration.
        /// </summary>
        public bool? KeepDeserialized
        {
            get;
            set;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        override public string ToString()
        {
            return typeof(PortableTypeConfiguration).Name + " [TypeName=" + TypeName + 
                ", NameMapper=" +  NameMapper + ", IdMapper=" + IdMapper + ", Serializer=" + Serializer +
                ", AffinityKeyFieldName=" + AffinityKeyFieldName + ']';
        }
    }
}
