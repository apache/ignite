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
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Metadata for particular type.
    /// </summary>
    internal class PortableMetadataHolder
    {
        /** Type ID. */
        private readonly int _typeId;

        /** Type name. */
        private readonly string _typeName;

        /** Affinity key field name. */
        private readonly string _affKeyFieldName;

        /** Empty metadata when nothig is know about object fields yet. */
        private readonly IPortableMetadata _emptyMeta;

        /** Collection of know field IDs. */
        private volatile ICollection<int> _ids;

        /** Last known unmodifiable metadata which is given to the user. */
        private volatile PortableMetadataImpl _meta;

        /** Saved flag (set if type metadata was saved at least once). */
        private volatile bool _saved;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        public PortableMetadataHolder(int typeId, string typeName, string affKeyFieldName)
        {
            _typeId = typeId;
            _typeName = typeName;
            _affKeyFieldName = affKeyFieldName;

            _emptyMeta = new PortableMetadataImpl(typeId, typeName, null, affKeyFieldName);
        }

        /// <summary>
        /// Get saved flag.
        /// </summary>
        /// <returns>True if type metadata was saved at least once.</returns>
        public bool Saved()
        {
            return _saved;
        }

        /// <summary>
        /// Get current type metadata.
        /// </summary>
        /// <returns>Type metadata.</returns>
        public IPortableMetadata Metadata()
        {
            PortableMetadataImpl meta0 = _meta;

            return meta0 != null ? _meta : _emptyMeta;
        }

        /// <summary>
        /// Currently cached field IDs.
        /// </summary>
        /// <returns>Cached field IDs.</returns>
        public ICollection<int> FieldIds()
        {
            ICollection<int> ids0 = _ids;

            if (_ids == null)
            {
                lock (this)
                {
                    ids0 = _ids;

                    if (ids0 == null)
                    {
                        ids0 = new HashSet<int>();

                        _ids = ids0;
                    }
                }
            }

            return ids0;
        }

        /// <summary>
        /// Merge newly sent field metadatas into existing ones.
        /// </summary>
        /// <param name="newMap">New field metadatas map.</param>
        public void Merge(IDictionary<int, Tuple<string, int>> newMap)
        {
            _saved = true;

            if (newMap == null || newMap.Count == 0)
                return;

            lock (this)
            {
                // 1. Create copies of the old meta.
                ICollection<int> ids0 = _ids;
                PortableMetadataImpl meta0 = _meta;

                ICollection<int> newIds = ids0 != null ? new HashSet<int>(ids0) : new HashSet<int>();

                IDictionary<string, int> newFields = meta0 != null ?
                    new Dictionary<string, int>(meta0.FieldsMap()) : new Dictionary<string, int>(newMap.Count);

                // 2. Add new fields.
                foreach (KeyValuePair<int, Tuple<string, int>> newEntry in newMap)
                {
                    if (!newIds.Contains(newEntry.Key))
                        newIds.Add(newEntry.Key);

                    if (!newFields.ContainsKey(newEntry.Value.Item1))
                        newFields[newEntry.Value.Item1] = newEntry.Value.Item2;
                }

                // 3. Assign new meta. Order is important here: meta must be assigned before field IDs.
                _meta = new PortableMetadataImpl(_typeId, _typeName, newFields, _affKeyFieldName);
                _ids = newIds;
            }
        }
    }
}
