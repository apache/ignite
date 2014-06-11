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
        /** <inheritdoc /> */
        public byte[] Marshal(Object val)
        {
            // TODO
            return null;
        }

        /** <inheritdoc /> */
        void Marshal(Object val, Stream output)
        {
            // TODO
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

            private Context()
            {
                // TODO
            }

            /**
             * <summary>Write object.</summary>
             * <param name="obj">Object.</param>
             */ 
            private void write(object obj)
            {
                if (obj is IGridClientPortable)
                    writePortable((IGridClientPortable)obj);
                else if (obj is IGridClientPortableEx)
                    writePortableEx((IGridClientPortableEx)obj);
                else
                    throw new GridClientPortableInvalidClassException("Object is neither IGridPortable nor IGridPortableEx: " + obj.GetType());

                // Finalize remaining references.
            }

            private void writePortable(IGridClientPortable obj)
            {
                if (frame != null)
                    frames.Push(frame);

                frame = new Frame();

                // TODO.

                frame = frames.Pop();
            }

            private void writePortableEx(IGridClientPortableEx obj)
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

    /** <summary>Writer which simply caches passed values.</summary> */
    private class DelayedWriter : IGridClientPortableWriter 
    {
        private readonly List<Action<IGridClientPortableWriter>> actions = new List<Action<IGridClientPortableWriter>>();

        private void execute(IGridClientPortableWriter writer)
        {
            foreach (Action<IGridClientPortableWriter> action in actions)
            {
                action.Invoke(writer);
            }
        }

        public void WriteByte(string fieldName, byte val)
        {
            throw new NotImplementedException(); 
        }

        public void WriteByte(byte val)
        {
            throw new NotImplementedException();
        }

        public void WriteByteArray(string fieldName, byte[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteByteArray(byte[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteChar(string fieldName, char val)
        {
            throw new NotImplementedException();
        }

        public void WriteChar(char val)
        {
            throw new NotImplementedException();
        }

        public void WriteCharArray(string fieldName, char[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteCharArray(char[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteShort(string fieldName, short val)
        {
            throw new NotImplementedException();
        }

        public void WriteShort(short val)
        {
            throw new NotImplementedException();
        }

        public void WriteShortArray(string fieldName, short[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteShortArray(short[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteInt(string fieldName, int val)
        {
            throw new NotImplementedException();
        }

        public void WriteInt(int val)
        {
            throw new NotImplementedException();
        }

        public void WriteIntArray(string fieldName, int[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteIntArray(int[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteLong(string fieldName, long val)
        {
            throw new NotImplementedException();
        }

        public void WriteLong(long val)
        {
            throw new NotImplementedException();
        }

        public void WriteLongArray(string fieldName, long[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteLongArray(long[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteBoolean(string fieldName, bool val)
        {
            throw new NotImplementedException();
        }

        public void WriteBoolean(bool val)
        {
            throw new NotImplementedException();
        }

        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteBooleanArray(bool[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteFloat(string fieldName, float val)
        {
            throw new NotImplementedException();
        }

        public void WriteFloat(float val)
        {
            throw new NotImplementedException();
        }

        public void WriteFloatArray(string fieldName, float[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteFloatArray(float[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteDouble(string fieldName, double val)
        {
            throw new NotImplementedException();
        }

        public void WriteDouble(double val)
        {
            throw new NotImplementedException();
        }

        public void WriteDoubleArray(string fieldName, double[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteDoubleArray(double[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteString(string fieldName, string val)
        {
            throw new NotImplementedException();
        }

        public void WriteString(string val)
        {
            throw new NotImplementedException();
        }

        public void WriteStringArray(string fieldName, string[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteStringArray(string[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteGuid(string fieldName, Guid val)
        {
            throw new NotImplementedException();
        }

        public void WriteGuid(Guid val)
        {
            throw new NotImplementedException();
        }

        public void WriteGuidArray(string fieldName, Guid[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteGuidArray(Guid[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteObject<T>(string fieldName, T val)
        {
            throw new NotImplementedException();
        }

        public void WriteObject<T>(T val)
        {
            throw new NotImplementedException();
        }

        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteObjectArray<T>(T[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteCollection<T>(string fieldName, ICollection<T> val)
        {
            throw new NotImplementedException();
        }

        public void WriteCollection<T>(ICollection<T> val)
        {
            throw new NotImplementedException();
        }

        public void WriteMap<K, V>(string fieldName, IDictionary<K, V> val)
        {
            throw new NotImplementedException();
        }

        public void WriteMap<K, V>(IDictionary<K, V> val)
        {
            throw new NotImplementedException();
        }
    }
}
