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

    [StructLayout(LayoutKind.Sequential)]
    internal struct PortableObjectSchemaField
    {
        public readonly int Id;
        public readonly int Offset;

        public PortableObjectSchemaField(int id, int offset)
        {
            Id = id;
            Offset = offset;
        }

        public static unsafe void WriteArray(PortableObjectSchemaField[] fields, IPortableStream stream, int count)
        {
            Debug.Assert(fields != null);
            Debug.Assert(stream != null);
            Debug.Assert(count > 0);

            if (BitConverter.IsLittleEndian)
            {
                fixed (PortableObjectSchemaField* ptr = &fields[0])
                {
                    stream.Write((byte*) ptr, count << 3); // 8 == sizeof (PortableObjectSchemaField)
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
            }
        }

        public static unsafe PortableObjectSchemaField[] ReadArray(IPortableStream stream, int size)
        {
            Debug.Assert(stream != null);
            Debug.Assert(size > 0);

            int fieldCount = size >> 3; // 8 == sizeof (PortableObjectSchemaField)

            var res = new PortableObjectSchemaField[fieldCount];

            if (BitConverter.IsLittleEndian)
            {
                fixed (PortableObjectSchemaField* ptr = &res[0])
                {
                    stream.Read((byte*) ptr, size); 
                }
            }
            else
            {
                for (int i = 0; i < fieldCount >> 3; i++) 
                {
                    res[i] = new PortableObjectSchemaField(stream.ReadInt(), stream.ReadInt());
                }
            }

            return res;
        }
    }
}