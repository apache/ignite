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

namespace Apache.Ignite.Core.Impl.Portable.Metadata
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable metadata implementation.
    /// </summary>
    public class PortableMetadataImpl : IPortableMetadata, IPortableWriteAware
    {
        /** Empty dictionary. */
        private static readonly IDictionary<string, int> EmptyDict = new Dictionary<string, int>();

        /** Empty list. */
        private static readonly ICollection<string> EmptyList = new List<string>().AsReadOnly();

        /** Fields. */
        private readonly IDictionary<string, int> _fields;
        
        /** */
        private readonly IIgniteContext _context;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableMetadataImpl" /> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public PortableMetadataImpl(IPortableReaderEx reader)
        {
            var rawReader = reader.RawReader();

            TypeId = rawReader.ReadInt();
            TypeName = rawReader.ReadString();
            AffinityKeyFieldName = rawReader.ReadString();
            _fields = rawReader.ReadGenericDictionary<string, int>();

            _context = reader.Marshaller.IgniteContext;
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="fields">Fields.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        /// <param name="context">The context.</param>
        public PortableMetadataImpl(int typeId, string typeName, IDictionary<string, int> fields,
            string affKeyFieldName, IIgniteContext context)
        {
            Debug.Assert(!string.IsNullOrEmpty(typeName));
            Debug.Assert(context != null);

            TypeId = typeId;
            TypeName = typeName;
            AffinityKeyFieldName = affKeyFieldName;
            _fields = fields;

            _context = context;
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        /// <returns></returns>
        public int TypeId { get; private set; }

        /// <summary>
        /// Gets type name.
        /// </summary>
        public string TypeName { get; private set; }

        /// <summary>
        /// Gets field names for that type.
        /// </summary>
        public ICollection<string> Fields
        {
            get { return _fields != null ? _fields.Keys : EmptyList; }
        }

        /// <summary>
        /// Gets field type for the given field name.
        /// </summary>
        /// <param name="fieldName">GetField name.</param>
        /// <returns>
        /// GetField type.
        /// </returns>
        public string GetFieldTypeName(string fieldName)
        {
            if (_fields != null)
            {
                int typeId;

                _fields.TryGetValue(fieldName, out typeId);

                return PortableUtils.ConvertTypeName(typeId, _context);
            }
            
            return null;
        }

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName { get; private set; }

        /// <summary>
        /// Gets fields map.
        /// </summary>
        /// <value>Fields map.</value>
        public IDictionary<string, int> FieldsMap
        {
            get { return _fields ?? EmptyDict; }
        }

        /// <summary>
        /// Writes this object to the given writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        public void WritePortable(IPortableWriterEx writer)
        {
            IPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteInt(TypeId);
            rawWriter.WriteString(TypeName);
            rawWriter.WriteString(AffinityKeyFieldName);
            rawWriter.WriteGenericDictionary(_fields);
        }
    }
}
