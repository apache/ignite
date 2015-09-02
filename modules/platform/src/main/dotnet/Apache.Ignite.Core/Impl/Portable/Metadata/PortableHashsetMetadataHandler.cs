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

    /// <summary>
    /// Metadata handler which uses hash set to determine whether field was already written or not.
    /// </summary>
    internal class PortableHashsetMetadataHandler : IPortableMetadataHandler
    {
        /** Empty fields collection. */
        private static readonly IDictionary<string, int> EmptyFields = new Dictionary<string, int>();

        /** IDs known when serialization starts. */
        private readonly ICollection<int> _ids;

        /** New fields. */
        private IDictionary<string, int> _fieldMap;

        /** */
        private readonly bool _newType;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ids">IDs.</param>
        /// <param name="newType">True is metadata for type is not saved.</param>
        public PortableHashsetMetadataHandler(ICollection<int> ids, bool newType)
        {
            _ids = ids;
            _newType = newType;
        }

        /** <inheritdoc /> */
        public void OnFieldWrite(int fieldId, string fieldName, int typeId)
        {
            if (!_ids.Contains(fieldId))
            {
                if (_fieldMap == null)
                    _fieldMap = new Dictionary<string, int>();

                if (!_fieldMap.ContainsKey(fieldName))
                    _fieldMap[fieldName] = typeId;
            }
        }

        /** <inheritdoc /> */
        public IDictionary<string, int> OnObjectWriteFinished()
        {
            return _fieldMap ?? (_newType ? EmptyFields : null);
        }
    }
}
