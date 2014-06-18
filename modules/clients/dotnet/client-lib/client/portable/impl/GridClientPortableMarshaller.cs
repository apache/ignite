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
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using GridGain.Client.Portable;

    /** <summary>Portable marshaller implementation.</summary> */
    internal class GridClientPortableMarshaller
    {
        /** Header of NULL object. */
        private static readonly byte HDR_NULL = 0x80;

        /** Header of object handle. */
        private static readonly byte HDR_HND = 0x81;

        /** Header of object in fully serialized form. */
        private static readonly byte HDR_FULL = 0x82;

        /** Header of object in fully serailized form with metadata. */
        private static readonly byte HDR_META = 0x83;

        /** <inheritdoc /> */
        public byte[] Marshal(object val)
        {
            GridClientPortableByteArrayMarshallerOutput output = new GridClientPortableByteArrayMarshallerOutput();

            // TODO: GG-8535: Pass context here.
            Marshal0(val, output, null);

            return output.Data();
        }

        /** <inheritdoc /> */
        void Marshal(object val, Stream output)
        {
            // TODO: GG-8535: Pass context here.
            Marshal0(val, new GridClientPortableStreamMarshallerOutput(output), null);
        }

        /** <inheritdoc /> */
        T Unmarshal<T>(byte[] data)
        {
            // TODO: GG-8535: Implement.
            return default(T);
        }

        /** <inheritdoc /> */
        T Unmarshal<T>(Stream input)
        {
            // TODO: GG-8535: Implement.
            return default(T);
        }

        /**
         * <summary>Internal marshalling routine.</summary>
         * <param name="obj">Object to be serialized.</param>
         * <param name="output">Output.</param>
         * <param name="ctx">Context.</param>
         */
        private void Marshal0(object obj, IGridClientPortableMarshallerOutput output, GridClientPortableSerializationContext ctx)
        {
            new Context(ctx, output).Write(obj);
        }
        
        /**
         * <summary>Context.</summary>
         */ 
        private class Context 
        {
            /** Per-connection serialization context. */
            private readonly GridClientPortableSerializationContext ctx;

            /** Output. */
            private readonly IGridClientPortableMarshallerOutput output;

            /** Tracking wrier. */
            private readonly Writer writer;

            /** Current length. */
            private int len;

            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Client connection context.</param>
             * <param name="output">Output.</param>
             */
            public Context(GridClientPortableSerializationContext ctx, IGridClientPortableMarshallerOutput output)
            {
                this.ctx = ctx;
                this.output = output;

                writer = new Writer(this);
            }

            /**
             * <summary>Write top-level object to the context.</summary>
             * <param name="obj">Object.</param>
             */ 
            public void Write(object obj)
            {
                if (obj == null)
                {
                    // Special case for top-level null value.
                    output.Initialize(1);

                    output.WriteByte(HDR_NULL);
                }
                else 
                { 
                    Frame frame = Write0(obj); // Top-level frame.

                    output.Initialize(frame.Length());

                    frame.Write(output);
                }

                output.Close();
            }

            /**
             * <summary>Internal write routine.</summary>
             * <param name="obj">Object to write.</param>
             */ 
            public Frame Write0(object obj)
            {
                Type type = obj.GetType();

                return obj.GetType().IsPrimitive ? new PrimitiveFrame(obj) as Frame : new ObjectFrame(this, obj);
            }
        }

        /**
         * <summary>Serialization frame.</summary>
         */ 
        private interface Frame
        {
            /**
             * <summary>Prepare frame.</summary>
             */ 
            void Prepare();

            /**
             * <summary>Get's frame data length.</summary>
             * <returns>Frame raw length.</returns>
             */ 
            int Length();

            /**
             * <summary>Write frame data to output.</summary>
             * <param name="output">Output.</param>
             */ 
            void Write(IGridClientPortableMarshallerOutput output);
        }

        /**
         * <summary>Primitive frame.</summary>
         */ 
        private class PrimitiveFrame : Frame
        {
            /** Primitive object. */
            private readonly object obj;

            /** Type ID. */
            private int typeId;

            /** Full length. */
            private int len; 

            /**
             * <summary>Constructor.</summary>
             * <param name="obj">Object.</param>
             */ 
            public PrimitiveFrame(object obj)
            {
                this.obj = obj;
            }

            /** <inheritdoc /> */
            public void Prepare()
            {
                typeId = GridClientPortableUilts.PrimitiveTypeId(obj.GetType());

                len = GridClientPortableUilts.PrimitiveLength(typeId);
            }

            /** <inheritdoc /> */
            public int Length()
            {
                return /** Flag. */ 1 + /** User type flag. */  1 +  /** Type ID. */ 4 + /** Data length. */ len;
            }

            /** <inheritdoc /> */
            public void Write(IGridClientPortableMarshallerOutput output)
            {
                output.WriteByte(HDR_FULL);
                GridClientPortableUilts.WriteBoolean(false, output);
                GridClientPortableUilts.WritePrimitive(typeId, obj, output);
            }
        }

        /**
         * <summary>Object frame.</summary>
         */ 
        private class ObjectFrame : Frame
        {
            /** Context. */
            private readonly Context ctx;

            /** Object to be serialized. */
            private readonly object obj;

            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Context.</param>
             * <param name="obj">Object.</param>
             */
            public ObjectFrame(Context ctx, object obj)
            {
                this.ctx = ctx;
                this.obj = obj;
            }

            /** <inheritdoc /> */
            public void Prepare()
            {
                throw new NotImplementedException();
            }

            /** <inheritdoc /> */
            public int Length()
            {
                throw new NotImplementedException();
            }

            /** <inheritdoc /> */
            public void Write(IGridClientPortableMarshallerOutput output)
            {
                throw new NotImplementedException();
            }
        }

        /**
         * <summary>Writer.</summary>
         */ 
        private class Writer : IGridClientPortableWriter 
        {
            /** Context. */
            private readonly Context ctx;

            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Context.</param>
             */ 
            public Writer(Context ctx)
            {
                this.ctx = ctx;
            }
        }
        
    }

    /** <summary>Writer which simply caches passed values.</summary> */
    public class DelayedWriter //: IGridClientPortableWriter 
    {
        ///** Named actions. */
        //private readonly List<Action<IGridClientPortableWriter>> namedActions = new List<Action<IGridClientPortableWriter>>();

        ///** Actions. */
        //private readonly List<Action<IGridClientPortableWriter>> actions = new List<Action<IGridClientPortableWriter>>();

        ///**
        // * <summary>Execute all tracked write actions.</summary>
        // * <param name="writer">Underlying real writer.</param>
        // */
        //private void execute(IGridClientPortableWriter writer)
        //{
        //    foreach (Action<IGridClientPortableWriter> namedAction in namedActions)
        //    {
        //        namedAction.Invoke(writer);
        //    }

        //    foreach (Action<IGridClientPortableWriter> action in actions)
        //    {
        //        action.Invoke(writer);
        //    }
        //}

        ///** <inheritdoc /> */
        //public void WriteByte(string fieldName, sbyte val)
        //{
        //    namedActions.Add((writer) => writer.WriteByte(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteByte(sbyte val)
        //{
        //    actions.Add((writer) => writer.WriteByte(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteByteArray(string fieldName, sbyte[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteByteArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteByteArray(sbyte[] val)
        //{
        //    actions.Add((writer) => writer.WriteByteArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteChar(string fieldName, char val)
        //{
        //    namedActions.Add((writer) => writer.WriteChar(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteChar(char val)
        //{
        //    actions.Add((writer) => writer.WriteChar(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCharArray(string fieldName, char[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteCharArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCharArray(char[] val)
        //{
        //    actions.Add((writer) => writer.WriteCharArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShort(string fieldName, short val)
        //{
        //    namedActions.Add((writer) => writer.WriteShort(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShort(short val)
        //{
        //    actions.Add((writer) => writer.WriteShort(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShortArray(string fieldName, short[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteShortArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteShortArray(short[] val)
        //{
        //    actions.Add((writer) => writer.WriteShortArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteInt(string fieldName, int val)
        //{
        //    namedActions.Add((writer) => writer.WriteInt(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteInt(int val)
        //{
        //    actions.Add((writer) => writer.WriteInt(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteIntArray(string fieldName, int[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteIntArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteIntArray(int[] val)
        //{
        //    actions.Add((writer) => writer.WriteIntArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLong(string fieldName, long val)
        //{
        //    namedActions.Add((writer) => writer.WriteLong(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLong(long val)
        //{
        //    actions.Add((writer) => writer.WriteLong(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLongArray(string fieldName, long[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteLongArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteLongArray(long[] val)
        //{
        //    actions.Add((writer) => writer.WriteLongArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBoolean(string fieldName, bool val)
        //{
        //    namedActions.Add((writer) => writer.WriteBoolean(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBoolean(bool val)
        //{
        //    actions.Add((writer) => writer.WriteBoolean(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBooleanArray(string fieldName, bool[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteBooleanArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteBooleanArray(bool[] val)
        //{
        //    actions.Add((writer) => writer.WriteBooleanArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloat(string fieldName, float val)
        //{
        //    namedActions.Add((writer) => writer.WriteFloat(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloat(float val)
        //{
        //    actions.Add((writer) => writer.WriteFloat(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloatArray(string fieldName, float[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteFloatArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteFloatArray(float[] val)
        //{
        //    actions.Add((writer) => writer.WriteFloatArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDouble(string fieldName, double val)
        //{
        //    namedActions.Add((writer) => writer.WriteDouble(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDouble(double val)
        //{
        //    actions.Add((writer) => writer.WriteDouble(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDoubleArray(string fieldName, double[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteDoubleArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteDoubleArray(double[] val)
        //{
        //    actions.Add((writer) => writer.WriteDoubleArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteString(string fieldName, string val)
        //{
        //    namedActions.Add((writer) => writer.WriteString(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteString(string val)
        //{
        //    actions.Add((writer) => writer.WriteString(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteStringArray(string fieldName, string[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteStringArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteStringArray(string[] val)
        //{
        //    actions.Add((writer) => writer.WriteStringArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuid(string fieldName, Guid val)
        //{
        //    namedActions.Add((writer) => writer.WriteGuid(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuid(Guid val)
        //{
        //    actions.Add((writer) => writer.WriteGuid(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuidArray(string fieldName, Guid[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteGuidArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteGuidArray(Guid[] val)
        //{
        //    actions.Add((writer) => writer.WriteGuidArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObject<T>(string fieldName, T val)
        //{
        //    namedActions.Add((writer) => writer.WriteObject(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObject<T>(T val)
        //{
        //    actions.Add((writer) => writer.WriteObject(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObjectArray<T>(string fieldName, T[] val)
        //{
        //    namedActions.Add((writer) => writer.WriteObjectArray(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteObjectArray<T>(T[] val)
        //{
        //    actions.Add((writer) => writer.WriteObjectArray(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection(string fieldName, ICollection val)
        //{
        //    namedActions.Add((writer) => writer.WriteCollection(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection(ICollection val)
        //{
        //    actions.Add((writer) => writer.WriteCollection(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection<T>(string fieldName, ICollection<T> val)
        //{
        //    namedActions.Add((writer) => writer.WriteCollection(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteCollection<T>(ICollection<T> val)
        //{
        //    actions.Add((writer) => writer.WriteCollection(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap(string fieldName, IDictionary val)
        //{
        //    namedActions.Add((writer) => writer.WriteMap(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap(IDictionary val)
        //{
        //    actions.Add((writer) => writer.WriteMap(val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap<K, V>(string fieldName, IDictionary<K, V> val)
        //{
        //    namedActions.Add((writer) => writer.WriteMap(fieldName, val));
        //}

        ///** <inheritdoc /> */
        //public void WriteMap<K, V>(IDictionary<K, V> val)
        //{
        //    actions.Add((writer) => writer.WriteMap(val));
        //}
    }
}
