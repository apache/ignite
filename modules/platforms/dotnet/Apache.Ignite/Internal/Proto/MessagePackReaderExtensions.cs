/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Proto
{
    using System;
    using System.Buffers;
    using System.Buffers.Binary;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using MessagePack;

    /// <summary>
    /// Extension methods for <see cref="MessagePackReader"/>.
    /// </summary>
    internal static class MessagePackReaderExtensions
    {
        /// <summary>
        /// Reads an object with specified type.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="type">Type.</param>
        /// <returns>Resulting object.</returns>
        public static object? ReadObject(this ref MessagePackReader reader, ClientDataType type)
        {
            switch (type)
            {
                case ClientDataType.Int8:
                    return reader.ReadByte();

                case ClientDataType.Int16:
                    return reader.ReadInt16();

                case ClientDataType.Int32:
                    return reader.ReadInt32();

                case ClientDataType.Int64:
                    return reader.ReadInt64();

                case ClientDataType.Float:
                    return reader.ReadSingle();

                case ClientDataType.Double:
                    return reader.ReadDouble();

                case ClientDataType.Uuid:
                    return reader.ReadGuid();

                case ClientDataType.String:
                    return reader.ReadString();

                default:
                    throw new IgniteClientException("Unsupported type: " + type);
            }
        }

        /// <summary>
        /// Skips multiple elements.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <param name="count">Element count to skip.</param>
        public static void Skip(this ref MessagePackReader reader, int count)
        {
            for (var i = 0; i < count; i++)
            {
                reader.Skip();
            }
        }

        /// <summary>
        /// Reads an UUID (RFC #4122) as <see cref="Guid"/>.
        /// <para />
        /// <see cref="Guid"/> uses a mixed-endian format which differs from UUID,
        /// see https://en.wikipedia.org/wiki/Universally_unique_identifier#Encoding.
        /// </summary>
        /// <param name="reader">Reader.</param>
        /// <returns>Guid.</returns>
        public static Guid ReadGuid(this ref MessagePackReader reader)
        {
            const int guidSize = 16;

            ValidateExtensionType(ref reader, ClientMessagePackType.Uuid, guidSize);

            ReadOnlySequence<byte> seq = reader.ReadRaw(guidSize);
            ReadOnlySpan<byte> jBytes = seq.FirstSpan;

            Debug.Assert(jBytes.Length == guidSize, "jBytes.Length == 16");

            // Hoist bounds checks.
            var k = jBytes[15];
            var a = BinaryPrimitives.ReadInt32BigEndian(jBytes);
            var b = BinaryPrimitives.ReadInt16BigEndian(jBytes[4..]);
            var c = BinaryPrimitives.ReadInt16BigEndian(jBytes[6..]);
            var d = jBytes[8];
            var e = jBytes[9];
            var f = jBytes[10];
            var g = jBytes[11];
            var h = jBytes[12];
            var i = jBytes[13];
            var j = jBytes[14];

            return new Guid(a, b, c, d, e, f, g, h, i, j, k);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ValidateExtensionType(
            ref MessagePackReader reader,
            ClientMessagePackType expectedType,
            int expectedLength)
        {
            ExtensionHeader hdr = reader.ReadExtensionFormatHeader();

            if (hdr.TypeCode != (int)expectedType)
            {
                throw new IgniteClientException(
                    $"Expected {expectedType} extension ({(int)expectedType}), but got {hdr.TypeCode}.");
            }

            if (hdr.Length != expectedLength)
            {
                throw new IgniteClientException(
                    $"Expected {expectedLength} bytes for {expectedType} extension, but got {hdr.Length}.");
            }
        }
    }
}
