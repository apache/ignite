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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Binary serializer which reflectively writes all fields except of ones with 
    /// <see cref="System.NonSerializedAttribute"/>.
    /// <para />
    /// Note that Java platform stores dates as a difference between current time 
    /// and predefined absolute UTC date. Therefore, this difference is always the 
    /// same for all time zones. .NET, in contrast, stores dates as a difference 
    /// between current time and some predefined date relative to the current time 
    /// zone. It means that this difference will be different as you change time zones. 
    /// To overcome this discrepancy Ignite always converts .Net date to UTC form 
    /// before serializing and allows user to decide whether to deserialize them 
    /// in UTC or local form using <c>ReadTimestamp(..., true/false)</c> methods in 
    /// <see cref="IBinaryReader"/> and <see cref="IBinaryRawReader"/>.
    /// This serializer always read dates in UTC form. It means that if you have
    /// local date in any field/property, it will be implicitly converted to UTC
    /// form after the first serialization-deserialization cycle. 
    /// </summary>
    public sealed class BinaryReflectiveSerializer : IBinarySerializer
    {
        /** Raw mode flag. */
        private bool _rawMode;

        /** In use flag. */
        private bool _isInUse;

        /** Force timestamp flag. */
        private bool _forceTimestamp;

        /// <summary>
        /// Write binary object.
        /// </summary>
        /// <param name="obj">Object.</param>
        /// <param name="writer">Binary writer.</param>
        public void WriteBinary(object obj, IBinaryWriter writer)
        {
            throw new NotSupportedException(GetType() + ".WriteBinary should not be called directly.");
        }

        /// <summary>
        /// Read binary object.
        /// </summary>
        /// <param name="obj">Instantiated empty object.</param>
        /// <param name="reader">Binary reader.</param>
        public void ReadBinary(object obj, IBinaryReader reader)
        {
            throw new NotSupportedException(GetType() + ".ReadBinary should not be called directly.");
        }

        /// <summary>
        /// Gets or value indicating whether raw mode serialization should be used.
        /// <para />
        /// Raw mode does not include field names, improving performance and memory usage.
        /// However, queries do not support raw objects.
        /// </summary>
        public bool RawMode
        {
            get { return _rawMode; }
            set
            {
                ThrowIfInUse();

                _rawMode = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether all DateTime values should be written as Timestamp.
        /// <para />
        /// Timestamp format is required for values used in SQL and for interoperation with other platforms.
        /// Only UTC values are supported in Timestamp format. Other values will cause an exception on write.
        /// <para />
        /// Normally serializer uses <see cref="IBinaryWriter.WriteObject{T}"/> for DateTime fields.
        /// This attribute changes the behavior to <see cref="IBinaryWriter.WriteTimestamp"/>.
        /// <para />
        /// See also <see cref="TimestampAttribute"/>.
        /// </summary>
        public bool ForceTimestamp
        {
            get { return _forceTimestamp; }
            set
            {
                ThrowIfInUse();

                _forceTimestamp = value;
            }
        }

        /// <summary>
        /// Registers the specified type.
        /// </summary>
        internal IBinarySerializerInternal Register(Type type, int typeId, IBinaryNameMapper converter,
            IBinaryIdMapper idMapper)
        {
            _isInUse = true;

            return new BinaryReflectiveSerializerInternal(_rawMode)
                .Register(type, typeId, converter, idMapper, _forceTimestamp);
        }

        /// <summary>
        /// Throws an exception if this instance is already in use.
        /// </summary>
        private void ThrowIfInUse()
        {
            if (_isInUse)
            {
                throw new InvalidOperationException(typeof(BinaryReflectiveSerializer).Name +
                                                    ".RawMode cannot be changed after first serialization.");
            }
        }
    }
}
