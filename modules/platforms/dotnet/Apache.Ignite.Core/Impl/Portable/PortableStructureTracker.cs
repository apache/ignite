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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Portable.Structure;

    /// <summary>
    /// Encapsulates logic for tracking field access and updating type descriptor structure.
    /// </summary>
    internal struct PortableStructureTracker
    {
        /** Current type structure. */
        private readonly IPortableTypeDescriptor _desc;

        /** Current type structure path index. */
        private int _curStructPath;

        /** Current type structure action index. */
        private int _curStructAction;

        /** Current type structure updates. */
        private List<PortableStructureUpdate> _curStructUpdates;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableStructureTracker"/> class.
        /// </summary>
        /// <param name="desc">The desc.</param>
        public PortableStructureTracker(IPortableTypeDescriptor desc)
        {
            _desc = desc;
            _curStructPath = 0;
            _curStructAction = 0;
            _curStructUpdates = null;
        }

        /// <summary>
        /// Gets the field ID.
        /// </summary>
        public int GetFieldId(string fieldName, byte fieldTypeId = 0)
        {
            _curStructAction++;

            if (_curStructUpdates == null)
            {
                var fieldId = _desc.TypeStructure.GetFieldId(fieldName, ref _curStructPath, _curStructAction);

                if (fieldId != 0)
                    return fieldId;
            }

            return GetNewFieldId(fieldName, fieldTypeId, _curStructAction);
        }

        /// <summary>
        /// Updates the type structure.
        /// </summary>
        public void UpdateStructure()
        {
            if (_curStructUpdates != null)
                _desc.UpdateStructure(_desc.TypeStructure, _curStructPath, _curStructUpdates);
        }

        /// <summary>
        /// Updates the type structure and metadata for the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public void UpdateStructureAndMeta(PortableWriterImpl writer)
        {
            if (_curStructUpdates != null)
            {
                _desc.UpdateStructure(_desc.TypeStructure, _curStructPath, _curStructUpdates);

                var marsh = writer.Marshaller;

                var metaHnd = marsh.GetMetadataHandler(_desc);

                if (metaHnd != null)
                {
                    foreach (var u in _curStructUpdates)
                        metaHnd.OnFieldWrite(u.FieldId, u.FieldName, u.FieldType);

                    var meta = metaHnd.OnObjectWriteFinished();

                    if (meta != null)
                        writer.SaveMetadata(_desc.TypeId, _desc.TypeName, _desc.AffinityKeyFieldName, meta);
                }
            }
        }

        /// <summary>
        /// Get ID for the new field and save structure update.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="fieldTypeId">Field type ID.</param>
        /// <param name="action">Action index.</param>
        /// <returns>
        /// Field ID.
        /// </returns>
        private int GetNewFieldId(string fieldName, byte fieldTypeId, int action)
        {
            var fieldId = PortableUtils.FieldId(_desc.TypeId, fieldName, _desc.NameConverter, _desc.Mapper);

            if (_curStructUpdates == null)
                _curStructUpdates = new List<PortableStructureUpdate>();

            _curStructUpdates.Add(new PortableStructureUpdate(fieldName, fieldId, fieldTypeId, action));

            return fieldId;
        }
    }
}