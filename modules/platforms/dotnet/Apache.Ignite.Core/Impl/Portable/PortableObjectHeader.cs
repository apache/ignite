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
    using System.IO;
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Impl.Portable.IO;

    [StructLayout(LayoutKind.Sequential)]
    internal struct PortableObjectHeader
    {
        private const int FlagUserType = 0x1;
        private const int FlagRawOnly = 0x2;

        public readonly byte Header;
        public readonly byte Version;
        public readonly short Flags;
        public readonly int Length;
        public readonly int TypeId;
        public readonly int HashCode;
        public readonly int SchemaId;
        public readonly int SchemaOffset;

        private PortableObjectHeader(IPortableStream stream, int position)
        {
            stream.Seek(position, SeekOrigin.Begin);

            Header = stream.ReadByte();
            Version = stream.ReadByte();
            Flags = stream.ReadShort();
            Length = stream.ReadInt();
            TypeId = stream.ReadInt();
            HashCode = stream.ReadInt();
            SchemaId = stream.ReadInt();
            SchemaOffset = stream.ReadInt();
        }

        public bool IsUserType
        {
            get { return (Flags & FlagUserType) == FlagUserType; }
        }

        public bool IsRawOnly
        {
            get { return (Flags & FlagRawOnly) == FlagRawOnly; }
        }

        public static PortableObjectHeader Read(IPortableStream stream, int position)
        {
            if (!BitConverter.IsLittleEndian) 
                return new PortableObjectHeader(stream, position);

            unsafe
            {
                var hdr = new PortableObjectHeader();
                stream.Read((byte*)&hdr, position, sizeof(PortableObjectHeader));

                return hdr;
            }
        }
    }
}
