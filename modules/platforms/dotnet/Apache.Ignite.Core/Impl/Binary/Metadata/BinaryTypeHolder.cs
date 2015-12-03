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

namespace Apache.Ignite.Core.Impl.Binary.Metadata
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Metadata for particular type.
    /// </summary>
    internal class BinaryTypeHolder
    {
        /** Type ID. */
        private readonly int _typeId;

        /** Type name. */
        private readonly string _typeName;

        /** Affinity key field name. */
        private readonly string _affKeyFieldName;

        /** Enum flag. */
        private readonly bool _isEnum;

        /** Collection of know field IDs. */
        private volatile HashSet<int> _ids;

        /** Last known unmodifiable metadata which is given to the user. */
        private volatile BinaryType _meta;

        /** Saved flag (set if type metadata was saved at least once). */
        private volatile bool _saved;


        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="affKeyFieldName">Affinity key field name.</param>
        public BinaryTypeHolder(int typeId, string typeName, string affKeyFieldName, bool isEnum)
        {
            _typeId = typeId;
            _typeName = typeName;
            _affKeyFieldName = affKeyFieldName;
            _isEnum = isEnum;
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
        /// Currently cached field IDs.
        /// </summary>
        /// <returns>Cached field IDs.</returns>
        public ICollection<int> GetFieldIds()
        {
            var ids0 = _ids;

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
                var ids0 = _ids;
                BinaryType meta0 = _meta;

                var newIds = ids0 != null ? new HashSet<int>(ids0) : new HashSet<int>();

                IDictionary<string, int> newFields = meta0 != null ?
                    new Dictionary<string, int>(meta0.GetFieldsMap()) : new Dictionary<string, int>(newMap.Count);

                // 2. Add new fields.
                foreach (KeyValuePair<int, Tuple<string, int>> newEntry in newMap)
                {
                    if (!newIds.Contains(newEntry.Key))
                        newIds.Add(newEntry.Key);

                    if (!newFields.ContainsKey(newEntry.Value.Item1))
                        newFields[newEntry.Value.Item1] = newEntry.Value.Item2;
                }

                // 3. Assign new meta. Order is important here: meta must be assigned before field IDs.
                _meta = new BinaryType(_typeId, _typeName, newFields, _affKeyFieldName, _isEnum);
                _ids = newIds;
            }
        }
    }
}
