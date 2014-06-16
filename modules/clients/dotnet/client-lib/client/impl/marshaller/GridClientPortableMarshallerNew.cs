/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Marshaller
{
    using System;
    using System.IO;
    using System.Collections.Generic;
    using GridGain.Client.Portable;

    /** <summary>Portable marshaller implementation.</summary> */
    internal class GridClientPortableMarshallerNew : IGridClientMarshaller
    {
        /**  */
        private static readonly int TYPE_BOOL = 0;

        private static readonly int TYPE_SBYTE = 1;

        private static readonly int TYPE_INT16 = 2;

        private static readonly int TYPE_INT32 = 3;

        private static readonly int TYPE_INT64 = 4;

        private static readonly int TYPE_CHAR = 5;

        private static readonly int TYPE_SINGLE = 6;

        private static readonly int TYPE_DOUBLE = 7;

        private static readonly int TYPE_STRING = 8;

        /** Header of NULL object. */
        private static readonly byte HDR_NULL = 0x80;

        /** Header of object handle. */
        private static readonly byte HDR_HND = 0x81;

        /** Header of object in fully serialized form. */
        private static readonly byte HDR_FULL = 0x82;

        /** <inheritdoc /> */
        public byte[] Marshal(object val)
        {
            return Marshal0(val);
        }

        /** <inheritdoc /> */
        void Marshal(object val, Stream output)
        {
            byte[] valBytes = Marshal0(val);

            output.Write(valBytes, 0, valBytes.Length);
        }

        /** <inheritdoc /> */
        T Unmarshal<T>(byte[] data)
        {
            // TODO
            return default(T);
        }

        /** <inheritdoc /> */
        T Unmarshal<T>(Stream input)
        {
            // TODO
            return default(T);
        }

        /**
         * <summary>Internal marshalling routine.</summary>
         */ 
        private byte[] Marshal0(object val)
        {
            Context ctx = new Context();
            // TODO
            return null;
        }

        /** <summary>Marshalling context.</summary> */
        private class Context
        {
            /** Rewrites which will be performed during the second pass over resulting array. */
            private readonly IDictionary<int, int> rewrites = new Dictionary<int, int>();

            /** Type handles. */
            private readonly IDictionary<TypeDescriptor, int> typeHnds = new Dictionary<TypeDescriptor, int>();

            /** Object handles. */
            private readonly IDictionary<object, int> hnds = new Dictionary<object, int>();

            /** Frames. */
            private readonly Stack<Frame> frames = new Stack<Frame>();

            private Frame frame;

            public Context()
            {
                // TODO
            }

            /**
             * <summary>Write object.</summary>
             * <param name="obj">Object.</param>
             */ 
            private void write(object obj)
            {
                // Prepare context. 
                prepare(obj);
            }

            /**
             * <summary>Prepare context to handle this object.</summary>
             */ 
            private void prepare(object obj)
            {
                
            }
        }

        /** <summary>Object marshalling context.</summary> */
        private class Frame
        {

            /**
             * <summary>Add value.</summary>
             * <param name="val">Value.</param>
             */
            private void Add(object val)
            {

            }

            /**
             * <summary>Add name value.</summary>
             * <param name="fieldName">Field name.</param>
             * <param name="val">Value.</param>
             */
            private void Add(string fieldName, object val)
            {

            }
        }

        /** <summary>Type descriptor consisting of type name and optional field names.</summary> */
        private class TypeDescriptor
        {
            /** <summary>Type ID.</summary> */
            private int TypeId
            {
                get;
                set;
            }

            /** <summary>Fields participating in marshalling.</summary> */
            private IList<string> Fields
            {
                get;
                set;
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is TypeDescriptor) {
                    TypeDescriptor that = (TypeDescriptor)obj;
                    
                    return TypeId.Equals(that.TypeId) && (Fields == null && that.Fields == null || Fields != null && Fields.Equals(that.Fields));
                }
                else
                    return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return TypeId + (Fields == null ? 0 : 31 * Fields.GetHashCode());
            }
        }

        
    }

    /** <summary>Byte array writer.</summary> */
    private class ByteWriter : IGridClientPortableWriter
    {
        /** Default array size. */
        private static readonly int DFLT_SIZE = 1024;

        /** Byte array. */
        private byte[] data;

        /** Length. */
        private uint len;

        /** Whether current array size has power of 2 size. */
        private bool power2;

        /**
         * <summary>Constructor allocating byte array of default size.</summary>
         */
        private ByteWriter() : this(DFLT_SIZE) { }

        /**
         * <summary>Constructor allocating byte array of the given size.</summary>
         * <param name="initLen">Array size.</param>
         */ 
        private ByteWriter(int initLen)
        {
            if (initLen <= 0)
                initLen = DFLT_SIZE;

            data = new byte[initLen];

            power2 = (initLen & (initLen - 1)) == 0;
        }

        /**
         * <summary>Resize byte array allocating more space.</summary>
         */ 
        private void resize()
        {
            int len = data.Length;

            if (!power2)
            {
                len--;
                len |= len >> 1;
                len |= len >> 2;
                len |= len >> 4;
                len |= len >> 8;
                len |= len >> 16;
                len++;

                power2 = true;
            }

            len = len << 1;

            if (len < data.Length)
                throw new OverflowException("Buffer size overflow.");

            byte[] newData = new byte[len];

            Array.Copy(data, newData, data.Length);

            data = newData;
        }
    }

    /** <summary>Writer which simply caches passed values.</summary> */
    private class DelayedWriter : IGridClientPortableWriter 
    {
        /** Named actions. */
        private readonly List<Action<IGridClientPortableWriter>> namedActions = new List<Action<IGridClientPortableWriter>>();

        /** Actions. */
        private readonly List<Action<IGridClientPortableWriter>> actions = new List<Action<IGridClientPortableWriter>>();

        /**
         * <summary>Execute all tracked write actions.</summary>
         * <param name="writer">Underlying real writer.</param>
         */
        private void execute(IGridClientPortableWriter writer)
        {
            foreach (Action<IGridClientPortableWriter> namedAction in namedActions)
            {
                namedAction.Invoke(writer);
            }

            foreach (Action<IGridClientPortableWriter> action in actions)
            {
                action.Invoke(writer);
            }
        }

        /** <inheritdoc /> */
        public void WriteByte(string fieldName, byte val)
        {
            namedActions.Add((writer) => writer.WriteByte(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteByte(byte val)
        {
            actions.Add((writer) => writer.WriteByte(val));
        }

        /** <inheritdoc /> */
        public void WriteByteArray(string fieldName, byte[] val)
        {
            namedActions.Add((writer) => writer.WriteByteArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteByteArray(byte[] val)
        {
            actions.Add((writer) => writer.WriteByteArray(val));
        }

        /** <inheritdoc /> */
        public void WriteChar(string fieldName, char val)
        {
            namedActions.Add((writer) => writer.WriteChar(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteChar(char val)
        {
            actions.Add((writer) => writer.WriteChar(val));
        }

        /** <inheritdoc /> */
        public void WriteCharArray(string fieldName, char[] val)
        {
            namedActions.Add((writer) => writer.WriteCharArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteCharArray(char[] val)
        {
            actions.Add((writer) => writer.WriteCharArray(val));
        }

        /** <inheritdoc /> */
        public void WriteShort(string fieldName, short val)
        {
            namedActions.Add((writer) => writer.WriteShort(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteShort(short val)
        {
            actions.Add((writer) => writer.WriteShort(val));
        }

        /** <inheritdoc /> */
        public void WriteShortArray(string fieldName, short[] val)
        {
            namedActions.Add((writer) => writer.WriteShortArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteShortArray(short[] val)
        {
            actions.Add((writer) => writer.WriteShortArray(val));
        }

        /** <inheritdoc /> */
        public void WriteInt(string fieldName, int val)
        {
            namedActions.Add((writer) => writer.WriteInt(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteInt(int val)
        {
            actions.Add((writer) => writer.WriteInt(val));
        }

        /** <inheritdoc /> */
        public void WriteIntArray(string fieldName, int[] val)
        {
            namedActions.Add((writer) => writer.WriteIntArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteIntArray(int[] val)
        {
            actions.Add((writer) => writer.WriteIntArray(val));
        }

        /** <inheritdoc /> */
        public void WriteLong(string fieldName, long val)
        {
            namedActions.Add((writer) => writer.WriteLong(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteLong(long val)
        {
            actions.Add((writer) => writer.WriteLong(val));
        }

        /** <inheritdoc /> */
        public void WriteLongArray(string fieldName, long[] val)
        {
            namedActions.Add((writer) => writer.WriteLongArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteLongArray(long[] val)
        {
            actions.Add((writer) => writer.WriteLongArray(val));
        }

        /** <inheritdoc /> */
        public void WriteBoolean(string fieldName, bool val)
        {
            namedActions.Add((writer) => writer.WriteBoolean(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteBoolean(bool val)
        {
            actions.Add((writer) => writer.WriteBoolean(val));
        }

        /** <inheritdoc /> */
        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            namedActions.Add((writer) => writer.WriteBooleanArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteBooleanArray(bool[] val)
        {
            actions.Add((writer) => writer.WriteBooleanArray(val));
        }

        /** <inheritdoc /> */
        public void WriteFloat(string fieldName, float val)
        {
            namedActions.Add((writer) => writer.WriteFloat(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteFloat(float val)
        {
            actions.Add((writer) => writer.WriteFloat(val));
        }

        /** <inheritdoc /> */
        public void WriteFloatArray(string fieldName, float[] val)
        {
            namedActions.Add((writer) => writer.WriteFloatArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteFloatArray(float[] val)
        {
            actions.Add((writer) => writer.WriteFloatArray(val));
        }

        /** <inheritdoc /> */
        public void WriteDouble(string fieldName, double val)
        {
            namedActions.Add((writer) => writer.WriteDouble(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteDouble(double val)
        {
            actions.Add((writer) => writer.WriteDouble(val));
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(string fieldName, double[] val)
        {
            namedActions.Add((writer) => writer.WriteDoubleArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(double[] val)
        {
            actions.Add((writer) => writer.WriteDoubleArray(val));
        }

        /** <inheritdoc /> */
        public void WriteString(string fieldName, string val)
        {
            namedActions.Add((writer) => writer.WriteString(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteString(string val)
        {
            actions.Add((writer) => writer.WriteString(val));
        }

        /** <inheritdoc /> */
        public void WriteStringArray(string fieldName, string[] val)
        {
            namedActions.Add((writer) => writer.WriteStringArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteStringArray(string[] val)
        {
            actions.Add((writer) => writer.WriteStringArray(val));
        }

        /** <inheritdoc /> */
        public void WriteGuid(string fieldName, Guid val)
        {
            namedActions.Add((writer) => writer.WriteGuid(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteGuid(Guid val)
        {
            actions.Add((writer) => writer.WriteGuid(val));
        }

        /** <inheritdoc /> */
        public void WriteGuidArray(string fieldName, Guid[] val)
        {
            namedActions.Add((writer) => writer.WriteGuidArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteGuidArray(Guid[] val)
        {
            actions.Add((writer) => writer.WriteGuidArray(val));
        }

        /** <inheritdoc /> */
        public void WriteObject<T>(string fieldName, T val)
        {
            namedActions.Add((writer) => writer.WriteObject(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteObject<T>(T val)
        {
            actions.Add((writer) => writer.WriteObject(val));
        }

        /** <inheritdoc /> */
        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            namedActions.Add((writer) => writer.WriteObjectArray(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteObjectArray<T>(T[] val)
        {
            actions.Add((writer) => writer.WriteObjectArray(val));
        }

        /** <inheritdoc /> */
        public void WriteCollection<T>(string fieldName, ICollection<T> val)
        {
            namedActions.Add((writer) => writer.WriteCollection(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteCollection<T>(ICollection<T> val)
        {
            actions.Add((writer) => writer.WriteCollection(val));
        }

        /** <inheritdoc /> */
        public void WriteMap<K, V>(string fieldName, IDictionary<K, V> val)
        {
            namedActions.Add((writer) => writer.WriteMap(fieldName, val));
        }

        /** <inheritdoc /> */
        public void WriteMap<K, V>(IDictionary<K, V> val)
        {
            actions.Add((writer) => writer.WriteMap(val));
        }
    }
}
