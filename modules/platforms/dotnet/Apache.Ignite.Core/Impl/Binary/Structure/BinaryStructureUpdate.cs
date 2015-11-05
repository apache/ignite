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
    /// <summary>
    /// Binary type structure update descriptor.
    /// </summary>
    internal class BinaryStructureUpdate
    {
        /** Field name. */
        private readonly string _fieldName;

        /** Field ID. */
        private readonly int _fieldId;

        /** Field type. */
        private readonly byte _fieldType;

        /** Field index. */
        private readonly int _idx;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="fieldId">Field ID.</param>
        /// <param name="fieldType">Field type.</param>
        /// <param name="idx">Index.</param>
        public BinaryStructureUpdate(string fieldName, int fieldId, byte fieldType, int idx)
        {
            _fieldName = fieldName;
            _fieldId = fieldId;
            _fieldType = fieldType;
            _idx = idx;
        }

        /// <summary>
        /// Field name.
        /// </summary>
        public string FieldName
        {
            get { return _fieldName; }
        }

        /// <summary>
        /// Field ID.
        /// </summary>
        public int FieldId
        {
            get { return _fieldId; }
        }

        /// <summary>
        /// Field type.
        /// </summary>
        public byte FieldType
        {
            get { return _fieldType; }
        }

        /// <summary>
        /// Index.
        /// </summary>
        public int Index
        {
            get { return _idx; }
        }
    }
}
