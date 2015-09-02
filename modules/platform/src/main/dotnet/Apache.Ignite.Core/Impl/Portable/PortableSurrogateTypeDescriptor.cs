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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Surrogate type descriptor. Used in cases when type if identified by name and is not provided in configuration.
    /// </summary>
    internal class PortableSurrogateTypeDescriptor : IPortableTypeDescriptor
    {
        /** Portable configuration. */
        private readonly PortableConfiguration cfg;

        /** Type ID. */
        private readonly int id;

        /** Type name. */
        private readonly string name;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Portable configuration.</param>
        /// <param name="id">Type ID.</param>
        public PortableSurrogateTypeDescriptor(PortableConfiguration cfg, int id)
        {
            this.cfg = cfg;
            this.id = id;
        }

        /// <summary>
        /// Constrcutor.
        /// </summary>
        /// <param name="cfg">Portable configuration.</param>
        /// <param name="name">Type name.</param>
        public PortableSurrogateTypeDescriptor(PortableConfiguration cfg, string name)
        {
            this.cfg = cfg;
            this.name = name;

            id = PortableUtils.TypeId(name, cfg.DefaultNameMapper, cfg.DefaultIdMapper);
        }

        /** <inheritDoc /> */
        public Type Type
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public int TypeId
        {
            get { return id; }
        }

        /** <inheritDoc /> */
        public string TypeName
        {
            get { return name; }
        }

        /** <inheritDoc /> */
        public bool UserType
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public bool MetadataEnabled
        {
            get { return cfg.DefaultMetadataEnabled; }
        }

        /** <inheritDoc /> */
        public bool KeepDeserialized
        {
            get { return cfg.DefaultKeepDeserialized; }
        }

        /** <inheritDoc /> */
        public IPortableNameMapper NameConverter
        {
            get { return cfg.DefaultNameMapper; }
        }

        /** <inheritDoc /> */
        public IPortableIdMapper Mapper
        {
            get { return cfg.DefaultIdMapper; }
        }

        /** <inheritDoc /> */
        public IPortableSerializer Serializer
        {
            get { return cfg.DefaultSerializer; }
        }

        /** <inheritDoc /> */
        public string AffinityKeyFieldName
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public object TypedHandler
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public PortableSystemWriteDelegate UntypedHandler
        {
            get { return null; }
        }
    }
}
