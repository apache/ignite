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
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Impl.Portable.IO;

    [StructLayout(LayoutKind.Sequential)]
    internal struct PortableObjectHeader : IEquatable<PortableObjectHeader>
    {
        public const int Size = 12;

        private const int FlagUserType = 0x1;
        private const int FlagRawOnly = 0x2;

        public readonly byte Header;
        public readonly byte Version;
        public readonly short Flags;
        public readonly int TypeId;
        public readonly int HashCode;
        public readonly int Length;
        public readonly int SchemaId;
        public readonly int SchemaOffset;

        public PortableObjectHeader(byte header, byte version, short flags, int typeId, int hashCode, int length, int schemaId, int schemaOffset)
        {
            Header = header;
            Version = version;
            Flags = flags;
            TypeId = typeId;
            HashCode = hashCode;
            Length = length;
            SchemaId = schemaId;
            SchemaOffset = schemaOffset;
        }

        public PortableObjectHeader(bool userType, int typeId, int hashCode, int length, int schemaId, int schemaOffset, bool rawOnly)
        {
            Header = PortableUtils.HdrFull;
            Version = PortableUtils.ProtoVer;
            
            Flags = (short) (userType ? FlagUserType : 0);

            if (rawOnly)
                Flags = (short) (Flags | FlagRawOnly);

            TypeId = typeId;
            HashCode = hashCode;
            Length = length;
            SchemaId = schemaId;
            SchemaOffset = schemaOffset;
        }

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

        public int GetRawOffset(int position, IPortableStream stream)
        {
            Debug.Assert(stream != null);

            if (!HasRawOffset)
                return SchemaOffset;

            stream.Seek(position + Length - 4, SeekOrigin.Begin);

            return stream.ReadInt();
        }

        public int SchemaFieldCount
        {
            get
            {
                if (IsRawOnly)
                    return 0;

                var schemaSize = Length - SchemaOffset;

                if (HasRawOffset)
                    schemaSize -= 4;

                return schemaSize >> 3;   // 8 == sizeof(PortableObjectSchemaField)
            }
        }

        public PortableObjectHeader ChangeHashCode(int hashCode)
        {
            return new PortableObjectHeader(Header, Version, Flags, TypeId, hashCode, Length, SchemaId, SchemaOffset);
        }

        public static unsafe void Write(PortableObjectHeader* hdr, IPortableStream stream, int position)
        {
            stream.Seek(position, SeekOrigin.Begin);

            Write(hdr, stream);
        }

        public static unsafe void Write(PortableObjectHeader* hdr, IPortableStream stream)
        {
            if (BitConverter.IsLittleEndian)
                stream.Write((byte*) hdr, Size);
            else
                hdr->Write(stream);
        }

        public static PortableObjectHeader Read(IPortableStream stream, int position)
        {
            stream.Seek(position, SeekOrigin.Begin);

            return Read(stream);
        }

        public static unsafe PortableObjectHeader Read(IPortableStream stream)
        {
            if (BitConverter.IsLittleEndian)
            {
                var hdr = new PortableObjectHeader();

                stream.Read((byte*) &hdr, Size);

                return hdr;
            }

            return new PortableObjectHeader(stream);
        }

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

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is PortableObjectHeader && Equals((PortableObjectHeader) obj);
        }

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

        public static bool operator ==(PortableObjectHeader left, PortableObjectHeader right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(PortableObjectHeader left, PortableObjectHeader right)
        {
            return !left.Equals(right);
        }
    }
}
