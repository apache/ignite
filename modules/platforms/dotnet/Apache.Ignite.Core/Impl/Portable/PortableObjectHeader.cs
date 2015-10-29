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
        /** Size, equals to sizeof(PortableObjectHeader) */
        public const int Size = 24;

        /** User type flag */
        private const int FlagUserType = 0x1;

        /** Raw only flag */
        private const int FlagRawOnly = 0x2;

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

        public bool IsUserType
        {
            get { return (Flags & FlagUserType) == FlagUserType; }
        }

        public bool IsRawOnly
        {
            get { return (Flags & FlagRawOnly) == FlagRawOnly; }
        }

        public bool HasRawOffset
        {
            get
            {
                // Odd amount of records in schema => raw offset is the very last 4 bytes in object.
                return !IsRawOnly && (((Length - SchemaOffset) >> 2) & 0x1) != 0x0;
            }
        }

        public int SchemaSize
        {
            get
            {
                if (IsRawOnly)
                    return 0;

                var schemaSize = Length - SchemaOffset;

                if (HasRawOffset)
                    schemaSize -= 4;

                return schemaSize;
            }
        }

        public int GetSchemaEnd(int position)
        {
            var res = position + Length;

            if (HasRawOffset)
                res -= 4;

            return res;
        }

        public int GetSchemaStart(int position)
        {
            return IsRawOnly ? GetSchemaEnd(position) : position + SchemaOffset;
        }

        public int GetRawOffset(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);

            if (!HasRawOffset)
                return SchemaOffset;

            stream.Seek(position + Length - 4, SeekOrigin.Begin);

            return stream.ReadInt();
        }

        public Dictionary<int, int> ReadSchemaAsDictionary(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);

            var schemaSize = SchemaSize;

            if (schemaSize == 0)
                return null;

            stream.Seek(position + SchemaOffset, SeekOrigin.Begin);

            var schema = new Dictionary<int, int>(schemaSize >> 3);

            for (var i = 0; i < schemaSize; i++)
                schema.Add(stream.ReadInt(), stream.ReadInt());

            return schema;
        }

        public PortableObjectSchemaField[] ReadSchema(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);

            var schemaSize = SchemaSize;

            if (schemaSize == 0)
                return null;

            stream.Seek(position + SchemaOffset, SeekOrigin.Begin);

            return PortableObjectSchemaField.ReadArray(stream, schemaSize);
        }

        public static unsafe void Write(PortableObjectHeader* hdr, IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);
            Debug.Assert(position >= 0);

            stream.Seek(position, SeekOrigin.Begin);

            Write(hdr, stream);
        }

        public static unsafe void Write(PortableObjectHeader* hdr, IPortableStream stream)
        {
            Debug.Assert(stream != null);

            if (BitConverter.IsLittleEndian)
                stream.Write((byte*) hdr, Size);
            else
                hdr->Write(stream);
        }

        public static PortableObjectHeader Read(IPortableStream stream, int position)
        {
            Debug.Assert(stream != null);
            Debug.Assert(position >= 0);

            stream.Seek(position, SeekOrigin.Begin);

            return Read(stream);
        }

        public static unsafe PortableObjectHeader Read(IPortableStream stream)
        {
            Debug.Assert(stream != null);

            if (BitConverter.IsLittleEndian)
            {
                var hdr = new PortableObjectHeader();

                stream.Read((byte*) &hdr, Size);

                Debug.Assert(hdr.Version == PortableUtils.ProtoVer);
                Debug.Assert(hdr.SchemaOffset <= hdr.Length);
                Debug.Assert(hdr.SchemaOffset >= Size);

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
