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
    using System.Linq;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Reads and writes <see cref="IBinaryEqualityComparer"/>.
    /// </summary>
    internal static class BinaryEqualityComparerSerializer
    {
        /// <summary>
        /// SwapSpace type.
        /// </summary>
        private enum Type : byte
        {
            None = 0,
            Array = 1,
            Field = 2
        }

        /// <summary>
        /// Writes an instance.
        /// </summary>
        public static void Write(IBinaryRawWriter writer, IBinaryEqualityComparer comparer)
        {
            if (comparer == null)
            {
                writer.WriteByte((byte) Type.None);
                return;
            }

            var arrCmp = comparer as BinaryArrayEqualityComparer;

            if (arrCmp != null)
            {
                writer.WriteByte((byte) Type.Array);
                return;
            }

            var fieldCmp = (BinaryFieldEqualityComparer) comparer;

            writer.WriteByte((byte) Type.Field);

            fieldCmp.Validate();

            writer.WriteInt(fieldCmp.FieldNames.Count);

            foreach (var field in fieldCmp.FieldNames)
                writer.WriteString(field);
        }

        /// <summary>
        /// Reads an instance.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static IEqualityComparer<IBinaryObject> Read(IBinaryRawReader reader)
        {
            var type = (Type) reader.ReadByte();

            switch (type)
            {
                case Type.None:
                    return null;

                case Type.Array:
                    return new BinaryArrayEqualityComparer();

                case Type.Field:
                    return new BinaryFieldEqualityComparer
                    {
                        FieldNames = Enumerable.Range(0, reader.ReadInt()).Select(x => reader.ReadString()).ToArray()
                    };

                default:
                    throw new ArgumentOutOfRangeException("reader", type, "Invalid EqualityComparer type code");
            }
        }
    }
}
