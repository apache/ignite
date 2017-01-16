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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Uses a set of binary object fields to calculate hash code and check equality.
    /// Not implemented for now, will be done as part of IGNITE-4397.
    /// </summary>
    internal class BinaryFieldEqualityComparer : IEqualityComparer<IBinaryObject>, IBinaryEqualityComparer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryFieldEqualityComparer"/> class.
        /// </summary>
        public BinaryFieldEqualityComparer()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryFieldEqualityComparer"/> class.
        /// </summary>
        /// <param name="fieldNames">The field names for comparison.</param>
        public BinaryFieldEqualityComparer(params string[] fieldNames)
        {
            IgniteArgumentCheck.NotNullOrEmpty(fieldNames, "fieldNames");

            FieldNames = fieldNames;
        }

        /// <summary>
        /// Gets or sets the field names to be used for equality comparison.
        /// </summary>
        public ICollection<string> FieldNames { get; set; }

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">The first object to compare.</param>
        /// <param name="y">The second object to compare.</param>
        /// <returns>
        /// true if the specified objects are equal; otherwise, false.
        /// </returns>
        public bool Equals(IBinaryObject x, IBinaryObject y)
        {
            throw new NotSupportedException(GetType() + "is not intended for direct usage.");
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. 
        /// </returns>
        public int GetHashCode(IBinaryObject obj)
        {
            throw new NotSupportedException(GetType() + "is not intended for direct usage.");
        }

        /** <inheritdoc /> */
        int IBinaryEqualityComparer.GetHashCode(IBinaryStream stream, int startPos, int length,
            BinaryObjectSchemaHolder schema, int schemaId, Marshaller marshaller, IBinaryTypeDescriptor desc)
        {
            Debug.Assert(stream != null);
            Debug.Assert(startPos >= 0);
            Debug.Assert(length >= 0);
            Debug.Assert(schema != null);
            Debug.Assert(marshaller != null);
            Debug.Assert(desc != null);

            Validate();

            stream.Flush();

            // Preserve stream position.
            var pos = stream.Position;

            var reader = marshaller.StartUnmarshal(stream, BinaryMode.ForceBinary);
            var fields = schema.GetFullSchema(schemaId);

            int hash = 0;

            foreach (var fieldName in FieldNames)
            {
                int fieldId = BinaryUtils.FieldId(desc.TypeId, fieldName, desc.NameMapper, desc.IdMapper);
                int fieldHash = 0;  // Null (missing) field hash code is 0.
                int fieldPos;

                if (fields.TryGetValue(fieldId, out fieldPos))
                {
                    stream.Seek(startPos + fieldPos - BinaryObjectHeader.Size, SeekOrigin.Begin);
                    var fieldVal = reader.Deserialize<object>();
                    fieldHash = fieldVal != null ? fieldVal.GetHashCode() : 0;
                }

                hash = 31 * hash + fieldHash;
            }

            // Restore stream position.
            stream.Seek(pos, SeekOrigin.Begin);

            return hash;
        }

        /// <summary>
        /// Validates this instance.
        /// </summary>
        public void Validate()
        {
            if (FieldNames == null || FieldNames.Count == 0)
                throw new IgniteException("BinaryFieldEqualityComparer.FieldNames can not be null or empty.");
        }
    }
}