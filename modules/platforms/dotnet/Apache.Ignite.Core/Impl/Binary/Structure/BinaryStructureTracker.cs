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

namespace Apache.Ignite.Core.Impl.Binary.Structure
{
    using System.Collections.Generic;

    /// <summary>
    /// Encapsulates logic for tracking field access and updating type descriptor structure.
    /// </summary>
    internal struct BinaryStructureTracker
    {
        /** Current type structure. */
        private readonly IBinaryTypeDescriptor _desc;

        /** Struct. */
        private readonly BinaryStructure _portStruct;

        /** Current type structure path index. */
        private int _curStructPath;

        /** Current type structure action index. */
        private int _curStructAction;

        /** Current type structure updates. */
        private List<BinaryStructureUpdate> _curStructUpdates;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryStructureTracker" /> class.
        /// </summary>
        /// <param name="desc">The desc.</param>
        /// <param name="portStruct">The structure to work with.</param>
        public BinaryStructureTracker(IBinaryTypeDescriptor desc, BinaryStructure portStruct)
        {
            _desc = desc;
            _portStruct = portStruct;
            _curStructPath = 0;
            _curStructAction = 0;
            _curStructUpdates = null;
        }

        /// <summary>
        /// Gets the current structure action.
        /// </summary>
        public int CurStructAction
        {
            get { return _curStructAction; }
        }

        /// <summary>
        /// Gets the field ID.
        /// </summary>
        public int GetFieldId(string fieldName, byte fieldTypeId = 0)
        {
            _curStructAction++;

            if (_curStructUpdates == null)
            {
                var fieldId = _portStruct.GetFieldId(fieldName, fieldTypeId, ref _curStructPath,
                    _curStructAction);

                if (fieldId != 0)
                    return fieldId;
            }

            return GetNewFieldId(fieldName, fieldTypeId, _curStructAction);
        }

        /// <summary>
        /// Updates the type structure.
        /// </summary>
        public void UpdateReaderStructure()
        {
            if (_curStructUpdates != null)
                _desc.UpdateReadStructure(_desc.ReaderTypeStructure, _curStructPath, _curStructUpdates);
        }

        /// <summary>
        /// Updates the type structure and metadata for the specified writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public void UpdateWriterStructure(BinaryWriter writer)
        {
            if (_curStructUpdates != null)
            {
                _desc.UpdateWriteStructure(_desc.WriterTypeStructure, _curStructPath, _curStructUpdates);

                var marsh = writer.Marshaller;

                var metaHnd = marsh.GetBinaryTypeHandler(_desc);

                if (metaHnd != null)
                {
                    foreach (var u in _curStructUpdates)
                        metaHnd.OnFieldWrite(u.FieldId, u.FieldName, u.FieldType);

                    var meta = metaHnd.OnObjectWriteFinished();

                    if (meta != null)
                        writer.SaveMetadata(_desc, meta);
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
            var fieldId = BinaryUtils.FieldId(_desc.TypeId, fieldName, _desc.NameMapper, _desc.IdMapper);

            if (_curStructUpdates == null)
                _curStructUpdates = new List<BinaryStructureUpdate>();

            _curStructUpdates.Add(new BinaryStructureUpdate(fieldName, fieldId, fieldTypeId, action));

            return fieldId;
        }
    }
}