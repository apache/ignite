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
    using System.Runtime.InteropServices;
    using Apache.Ignite.Core.Impl.Portable.IO;

    /// <summary>
    /// Portable schema field DTO (as it is stored in a stream).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct PortableObjectSchemaField
    {
        /* Field ID */
        public readonly int Id;

        /** Offset. */
        public readonly int Offset;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableObjectSchemaField"/> struct.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <param name="offset">The offset.</param>
        public PortableObjectSchemaField(int id, int offset)
        {
            Id = id;
            Offset = offset;
        }

        /// <summary>
        /// Writes an array of fields to a stream.
        /// </summary>
        /// <param name="fields">Fields.</param>
        /// <param name="stream">Stream.</param>
        /// <param name="count">Field count to write.</param>
        /// <param name="maxOffset">The maximum field offset to determine whether 1, 2 or 4 bytes are needed for offsets.</param>
        /// <returns>
        /// Flags according to offset sizes: <see cref="PortableObjectHeader.FlagByteOffsets"/>, 
        /// <see cref="PortableObjectHeader.FlagShortOffsets"/>, or 0.
        /// </returns>
        public static unsafe short WriteArray(PortableObjectSchemaField[] fields, IPortableStream stream, int count, 
            int maxOffset)
        {
            Debug.Assert(fields != null);
            Debug.Assert(stream != null);
            Debug.Assert(count > 0);

            unchecked
            {
                if (maxOffset <= byte.MaxValue)
                {
                    for (int i = 0; i < count; i++)
                    {
                        var field = fields[i];

                        stream.WriteInt(field.Id);
                        stream.WriteByte((byte) field.Offset);
                    }

                    return PortableObjectHeader.FlagByteOffsets;
                }

                if (maxOffset <= ushort.MaxValue)
                {
                    for (int i = 0; i < count; i++)
                    {
                        var field = fields[i];

                        stream.WriteInt(field.Id);

                        stream.WriteShort((short) field.Offset);
                    }

                    return PortableObjectHeader.FlagShortOffsets;
                }
                
                for (int i = 0; i < count; i++)
                {
                    var field = fields[i];

                    stream.WriteInt(field.Id);
                    stream.WriteInt(field.Offset);
                }

                return 0;
            }

            /*
            if (BitConverter.IsLittleEndian)
            {
                fixed (PortableObjectSchemaField* ptr = &fields[0])
                {
                    stream.Write((byte*) ptr, count * Size);
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    var field = fields[i];

                    stream.WriteInt(field.Id);
                    stream.WriteInt(field.Offset);
                }
            }*/
        }


        /*
        /// <summary>
        /// Reads an array of fields from a stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="count">Count.</param>
        /// <returns></returns>
        public static unsafe PortableObjectSchemaField[] ReadArray(IPortableStream stream, int count)
        {
            Debug.Assert(stream != null);
            Debug.Assert(count > 0);

            var res = new PortableObjectSchemaField[count];

            if (BitConverter.IsLittleEndian)
            {
                fixed (PortableObjectSchemaField* ptr = &res[0])
                {
                    stream.Read((byte*) ptr, count * Size);
                }
            }
            else
            {
                for (int i = 0; i < count; i++) 
                {
                    res[i] = new PortableObjectSchemaField(stream.ReadInt(), stream.ReadInt());
                }
            }

            return res;
        }*/
    }
}