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
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Binary field metadata.
    /// </summary>
    internal struct BinaryField
    {
        /** Type ID. */
        private readonly int _typeId;

        /** Field ID. */
        private readonly int _fieldId;

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryField" /> class.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="fieldId">Field ID.</param>
        public BinaryField(int typeId, int fieldId)
        {
            _typeId = typeId;
            _fieldId = fieldId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryField" /> class.
        /// </summary>
        /// <param name="reader">Reader.</param>
        public BinaryField(IBinaryRawReader reader)
        {
            _typeId = reader.ReadInt();
            _fieldId = reader.ReadInt();
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        /// <returns>Type ID</returns>
        public int TypeId
        {
            get { return _typeId; }
        }

        /// <summary>
        /// Field ID.
        /// </summary>
        /// <returns>Field ID</returns>
        public int FieldId
        {
            get { return _fieldId; }
        }
    }
}
