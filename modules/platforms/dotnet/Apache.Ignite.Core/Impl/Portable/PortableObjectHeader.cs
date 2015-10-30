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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Portable object header structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct PortableObjectHeader : IEquatable<PortableObjectHeader>
    {
        /** Size, equals to sizeof(PortableObjectHeader). */
        public const int Size = 24;

        /** User type flag. */
        private const int FlagUserType = 0x1;

        /** Raw only flag. */
        private const int FlagRawOnly = 0x2;

        /** Byte-sized field offsets flag. */
        private const int FlagByteOffsets = 0x4;

        /** Short-sized field offsets flag. */
        private const int FlagShortOffsets = 0x8;

        /** Actual header layout */
        public readonly byte Header;        // Header code, always 103 (HdrFull)
        public readonly byte Version;       // Protocol version
        public readonly short Flags;        // Flags
        public readonly int TypeId;         // Type ID
        public readonly int HashCode;       // Hash code
        public readonly int Length;         // Length, including header
        public readonly int SchemaId;       // Schema ID (Fnv1 of field type ids)
        public readonly int SchemaOffset;   // Schema offset, or raw offset when RawOnly flag is set.

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableObjectHeader"/> struct.
        /// </summary>
        /// <param name="userType">User type flag.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="hashCode">Hash code.</param>
        /// <param name="length">Length.</param>
        /// <param name="schemaId">Schema ID.</param>
        /// <param name="schemaOffset">Schema offset.</param>
        /// <param name="rawOnly">Raw flag.</param>
        public PortableObjectHeader(bool userType, int typeId, int hashCode, int length, int schemaId, int schemaOffset, bool rawOnly)
        {
            Header = PortableUtils.HdrFull;
            Version = PortableUtils.ProtoVer;

            Debug.Assert(schemaOffset <= length);
            Debug.Assert(schemaOffset >= Size);
            
            Flags = (short) (userType ? FlagUserType : 0);

            if (rawOnly)
                Flags = (short) (Flags | FlagRawOnly);

            TypeId = typeId;
            HashCode = hashCode;
            Length = length;
            SchemaId = schemaId;
            SchemaOffset = schemaOffset;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableObjectHeader"/> struct from specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        private PortableObjectHeader(IPortableStream stream)
        {
            Header = stream.ReadByte();
            Version = stream.ReadByte();
            Flags = stream.ReadShort();
            Length = stream.ReadInt();
            TypeId = stream.ReadInt();
            HashCode = stream.ReadInt();
            SchemaId = stream.ReadInt();
            SchemaOffset = stream.ReadInt();
        }

        /// <summary>
        /// Writes this instance to the specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        private void Write(IPortableStream stream)
        {
            stream.WriteByte(Header);
            stream.WriteByte(Version);
            stream.WriteShort(Flags);
            stream.WriteInt(Length);
            stream.WriteInt(TypeId);
            stream.WriteInt(HashCode);
            stream.WriteInt(SchemaId);
            stream.WriteInt(SchemaOffset);
        }

        /// <summary>
        /// Gets a user type flag.
        /// </summary>
        public bool IsUserType
        {
            get { return (Flags & FlagUserType) == FlagUserType; }
        }

        /// <summary>
        /// Gets a raw-only flag.
        /// </summary>
        public bool IsRawOnly
        {
            get { return (Flags & FlagRawOnly) == FlagRawOnly; }
        }

        /// <summary>
        /// Gets a value indicating whether this instance has raw offset.
        /// </summary>
        public bool HasRawOffset
        {
            get
            {
                // Remainder => raw offset is the very last 4 bytes in object.
                return !IsRawOnly && ((Length - SchemaOffset) % SchemaFieldSize) == 4;
            }
        }

        /// <summary>
        /// Gets the size of the schema field offset (1, 2 or 4 bytes).
        /// </summary>
        public int SchemaFieldOffsetSize
        {
            get
            {
                if ((Flags & FlagByteOffsets) == FlagByteOffsets)
                    return 1;

                if ((Flags & FlagShortOffsets) == FlagShortOffsets)
                    return 2;

                return 4;
            }
        }

        /// <summary>
        /// Gets the size of the schema field.
        /// </summary>
        public int SchemaFieldSize
        {
            get { return SchemaFieldOffsetSize + 4; }
        }

        /// <summary>
        /// Gets the schema field count.
        /// </summary>
        public int SchemaFieldCount
        {
            get
            {
                if (IsRawOnly)
                    return 0;

                var schemaSize = Length - SchemaOffset;

                if (HasRawOffset)
                    schemaSize -= 4;

                return schemaSize >> 3;  // 8 == PortableObjectSchemaField.Size
            }
        }

        /// <summary>
        /// Gets the schema end.
        /// </summary>
        public int GetSchemaEnd(int position)
        {
            var res = position + Length;

            if (HasRawOffset)
                res -= 4;

            return res;
        }

        /// <summary>
        /// Gets the schema start.
        /// </summary>
        public int GetSchemaStart(int position)
        {
            return IsRawOnly ? GetSchemaEnd(position) : position + SchemaOffset;
        }

        /// <summary>
        /// Gets the raw offset of this object in specified stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Raw offset.</returns>
        public int GetRawOffset(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);

            if (!HasRawOffset)
                return SchemaOffset;

            stream.Seek(position + Length - 4, SeekOrigin.Begin);

            return stream.ReadInt();
        }

        /// <summary>
        /// Reads the schema as dictionary according to this header data.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Schema.</returns>
        public Dictionary<int, int> ReadSchemaAsDictionary(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);

            var schemaSize = SchemaFieldCount;

            if (schemaSize == 0)
                return null;

            stream.Seek(position + SchemaOffset, SeekOrigin.Begin);

            var schema = new Dictionary<int, int>(schemaSize);

            for (var i = 0; i < schemaSize; i++)
                schema.Add(stream.ReadInt(), stream.ReadInt());

            return schema;
        }

        /// <summary>
        /// Reads the schema according to this header data.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Schema.</returns>
        public PortableObjectSchemaField[] ReadSchema(IPortableStream stream, int position)
        {
            // TODO: This does not work
            Debug.Assert(stream != null);

            var schemaSize = SchemaFieldCount;

            if (schemaSize == 0)
                return null;

            stream.Seek(position + SchemaOffset, SeekOrigin.Begin);

            return PortableObjectSchemaField.ReadArray(stream, schemaSize);
        }

        /// <summary>
        /// Writes specified header to a stream.
        /// </summary>
        /// <param name="header">The header.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        public static unsafe void Write(PortableObjectHeader header, IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);
            Debug.Assert(position >= 0);

            stream.Seek(position, SeekOrigin.Begin);

            if (BitConverter.IsLittleEndian)
                stream.Write((byte*) &header, Size);
            else
                header.Write(stream);
        }

        /// <summary>
        /// Reads an instance from stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="position">The position.</param>
        /// <returns>Instance of the header.</returns>
        public static unsafe PortableObjectHeader Read(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);
            Debug.Assert(position >= 0);

            stream.Seek(position, SeekOrigin.Begin);

            if (BitConverter.IsLittleEndian)
            {
                var hdr = new PortableObjectHeader();

                stream.Read((byte*) &hdr, Size);

                Debug.Assert(hdr.Version == PortableUtils.ProtoVer);
                Debug.Assert(hdr.SchemaOffset <= hdr.Length);
                Debug.Assert(hdr.SchemaOffset >= Size);

                // Only one of the flags can be set
                var f = hdr.Flags;
                Debug.Assert((f & (FlagShortOffsets | FlagByteOffsets)) != (FlagShortOffsets | FlagByteOffsets));

                return hdr;
            }

            return new PortableObjectHeader(stream);
        }

        /** <inheritdoc> */
        public bool Equals(PortableObjectHeader other)
        {
            return Header == other.Header &&
                   Version == other.Version &&
                   Flags == other.Flags &&
                   TypeId == other.TypeId &&
                   HashCode == other.HashCode &&
                   Length == other.Length &&
                   SchemaId == other.SchemaId &&
                   SchemaOffset == other.SchemaOffset;
        }

        /** <inheritdoc> */
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            
            return obj is PortableObjectHeader && Equals((PortableObjectHeader) obj);
        }

        /** <inheritdoc> */
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Header.GetHashCode();
                hashCode = (hashCode*397) ^ Version.GetHashCode();
                hashCode = (hashCode*397) ^ Flags.GetHashCode();
                hashCode = (hashCode*397) ^ TypeId;
                hashCode = (hashCode*397) ^ HashCode;
                hashCode = (hashCode*397) ^ Length;
                hashCode = (hashCode*397) ^ SchemaId;
                hashCode = (hashCode*397) ^ SchemaOffset;
                return hashCode;
            }
        }

        /** <inheritdoc> */
        public static bool operator ==(PortableObjectHeader left, PortableObjectHeader right)
        {
            return left.Equals(right);
        }

        /** <inheritdoc> */
        public static bool operator !=(PortableObjectHeader left, PortableObjectHeader right)
        {
            return !left.Equals(right);
        }
    }
}
