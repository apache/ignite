/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace GridGain.Client.Impl.Marshaller {
    /** <summary></summary> */
    internal class GridClientPortableMarshaller : IGridClientMarshaller {
        private const byte TYPE_NULL = 0;

        private const byte TYPE_BYTE = 1;

        private const byte TYPE_SHORT = 2;

        private const byte TYPE_INT = 3;

        private const byte TYPE_LONG = 4;

        private const byte TYPE_FLOAT = 5;

        private const byte TYPE_DOUBLE = 6;

        private const byte TYPE_BOOLEAN = 7;

        private const byte TYPE_CHAR = 8;

        private const byte TYPE_STRING = 9;

        private const byte TYPE_BYTE_ARRAY = 10;

        private const byte TYPE_SHORT_ARRAY = 11;

        private const byte TYPE_INT_ARRAY = 12;

        private const byte TYPE_LONG_ARRAY = 13;

        private const byte TYPE_FLOAT_ARRAY = 14;

        private const byte TYPE_DOUBLE_ARRAY = 15;

        private const byte TYPE_BOOLEAN_ARRAY = 16;

        private const byte TYPE_CHAR_ARRAY = 17;

        private const byte TYPE_LIST = 18;

        private const byte TYPE_MAP = 19;

        private const byte TYPE_UUID = 20;

        private const byte TYPE_USER_OBJECT = 21;

        private const byte OBJECT_TYPE_OBJECT = 0;

        private const byte OBJECT_TYPE_REF = 1;

        private const byte OBJECT_TYPE_NULL = 2;

        private IDictionary<int, Type> portableTypes;

        public GridClientPortableMarshaller(IDictionary<int, Type> portableTypes) {
            this.portableTypes = portableTypes;
        }

        /** <inheritdoc /> */
        public byte[] Marshal(Object val) {
            var portable = (IGridPortableObject) val;

            var writer = new Writer();

            writer.WritePortable(portable);

            return writer.Bytes();
        }

        /** <inheritdoc /> */
        public T Unmarshal<T>(byte[] data) {
            return (T)new Reader(portableTypes, data).readPortable();
        }

        /** <inheritdoc /> */
        public void Marshal(object val, Stream output) {
            byte[] bytes = Marshal(val);

            output.Write(bytes, 0, bytes.Length);
        }

        /** <inheritdoc /> */
        public T Unmarshal<T>(Stream input) {
            return (T) (new Object()); // TODO 8535 (not used now).
        }

        private class ByteArrayOutput {
            private byte[] bytes;

            private int pos;

            public ByteArrayOutput(int capacity) {
                bytes = new byte[capacity];
            }

            public byte[] Bytes() {
                if (bytes.Length != pos) {
                    var res = new byte[pos];

                    Array.Copy(bytes, 0, res, 0, pos);

                    return res;
                }

                return bytes;
            }

            public void WriteByte(byte val) {
                EnsureCapacity(1);

                bytes[pos++] = val;
            }

            public void WriteBytes(byte[] val) {
                EnsureCapacity(val.Length);

                Array.Copy(val, 0, bytes, pos, val.Length);

                pos += val.Length;
            }

            public void WriteInt(int val) {
                EnsureCapacity(4);

                // TODO: can use the same code as BitConverter?

                bytes[pos++] = (byte) (val & byte.MaxValue);
                bytes[pos++] = (byte) ((uint) val >> 8 & byte.MaxValue);
                bytes[pos++] = (byte) ((uint) val >> 16 & byte.MaxValue);
                bytes[pos++] = (byte) ((uint) val >> 24 & byte.MaxValue);
            }

            private void EnsureCapacity(int size) {
                if (size + pos > bytes.Length) {
                    var newBytes = new byte[bytes.Length*2 + size];

                    Array.Copy(bytes, 0, newBytes, 0, pos);

                    bytes = newBytes;
                }
            }
        }

        private class Writer : IGridPortableWriter {
            private readonly ByteArrayOutput output;

            internal Writer() {
                output = new ByteArrayOutput(1024);
            }

            public void WriteInt(string fieldName, int val) {
                output.WriteInt(val);
            }

            public void WriteString(string fieldName, string val) {
                if (val != null) {
                    byte[] strBytes = Encoding.UTF8.GetBytes(val);

                    output.WriteInt(strBytes.Length);
                    output.WriteBytes(strBytes);
                }
                else
                    output.WriteInt(-1);
            }

            public void WriteBoolean(string fieldName, bool val) {
                throw new NotImplementedException();
            }

            public void WriteShort(string fieldName, short val) {
                throw new NotImplementedException();
            }

            public void WriteChar(string fieldName, char val) {
                throw new NotImplementedException();
            }

            public void WriteLong(string fieldName, long val) {
                throw new NotImplementedException();
            }

            public void WriteFloat(string fieldName, float val) {
                throw new NotImplementedException();
            }

            public void WriteDouble(string fieldName, double val) {
                throw new NotImplementedException();
            }

            public void WriteBytes(string fieldName, byte[] val) {
                throw new NotImplementedException();
            }

            public void WriteObject(string fieldName, object val) {
                throw new NotImplementedException();
            }

            public void WriteMap<TKey, TVal>(string fieldName, IDictionary<TKey, TVal> val)
            {
                throw new NotImplementedException();
            }

            public void WriteCollection<T>(string fieldName, IEnumerable<T> val) {
                throw new NotImplementedException();
            }

            public void WriteGuid(string fieldName, Guid val) {
                throw new NotImplementedException();
            }

            public byte[] Bytes() {
                return output.Bytes();
            }

            public void WritePortable(IGridPortableObject portable) {
                output.WriteByte(OBJECT_TYPE_OBJECT);

                output.WriteInt(portable.TypeId);

                portable.WritePortable(this);
            }
        }

        private class Reader : IGridPortableReader {
            private readonly IDictionary<int, Type> portableTypes;

            private readonly ByteArrayInput input;

            public Reader(IDictionary<int, Type> portableTypes, byte[] bytes) {
                this.portableTypes = portableTypes;

                input = new ByteArrayInput(bytes);
            }

            internal IGridPortableObject readPortable() {
                byte type = input.ReadByte();

                if (type != OBJECT_TYPE_OBJECT)
                    throw new IOException("Invalid type: " + type); // TODO 8535.

                int typeId = input.ReadInt();

                Type portableType = portableTypes[typeId];

                if (portableType == null)
                    throw new IOException("No type for portable typeId: " + typeId);

                IGridPortableObject portable = (IGridPortableObject)Activator.CreateInstance(portableType);

                portable.ReadPortable(this);

                return portable;
            }

            public int ReadInt(string fieldName) {
                return input.ReadInt();
            }

            public string ReadString(string fieldName) {
                int len = input.ReadInt();

                if (len == -1)
                    return null;

                byte[] strBytes = input.ReadBytes(len);

                return Encoding.UTF8.GetString(strBytes);
            }

            public bool ReadBoolean(string fieldName) {
                throw new NotImplementedException();
            }

            public short ReadShort(string fieldName) {
                throw new NotImplementedException();
            }

            public char ReadChar(string fieldName) {
                throw new NotImplementedException();
            }

            public long ReadLong(string fieldName) {
                throw new NotImplementedException();
            }

            public float ReadFloat(string fieldName) {
                throw new NotImplementedException();
            }

            public double ReadDouble(string fieldName) {
                throw new NotImplementedException();
            }

            public byte[] ReadBytes(string fieldName) {
                throw new NotImplementedException();
            }

            public T ReadObject<T>(string fieldName) {
                throw new NotImplementedException();
            }

            public IDictionary<TKey, TVal> ReadMap<TKey, TVal>(string fieldName) {
                throw new NotImplementedException();
            }

            public ICollection<T> ReadCollection<T>(string fieldName) {
                throw new NotImplementedException();
            }

            public Guid ReadGuid(string fieldName) {
                throw new NotImplementedException();
            }
        }

        private class ByteArrayInput {
            private readonly byte[] bytes;

            private int pos;

            internal ByteArrayInput(byte[] bytes) {
                this.bytes = bytes;
            }

            internal byte ReadByte() {
                return bytes[pos++];
            }

            internal int ReadInt()
            {
                int b1 = bytes[pos++];
                int b2 = bytes[pos++];
                int b3 = bytes[pos++];
                int b4 = bytes[pos++];

                return (b4 << 24) + (b3 << 16) + (b2 << 8) + b1;
            }

            internal byte[] ReadBytes(int len) {
                byte[] res = new byte[len];

                Array.Copy(bytes, pos, res, 0, len);

                pos += len;

                return res;
            }
        }
    }
}
