/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
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

        /**
         * <summary>Marhshal object</summary>
         * <param name="val">Value.</param>
         * <param name="ctx">Serialization context.</param>
         * <returns>Serialized data as byte array.</returns>
         */
        public byte[] Marshal(object val, GridClientPortableSerializationContext ctx)
        {
            MemoryStream stream = new MemoryStream();

            Marshal(val, stream, ctx);

            return stream.ToArray();
        }

        /**
         * <summary>Marhshal object</summary>
         * <param name="val">Value.</param>
         * <param name="stream">Output stream.</param>
         * <param name="ctx">Serialization context.</param>
         */
        public void Marshal(object val, Stream stream, GridClientPortableSerializationContext ctx)
        {
            new Context(ctx, stream).Write(val);
        }

        /**
         * 
         */ 
        public T Unmarshal<T>(byte[] data)
        {
            // TODO: GG-8535: Implement.
            return default(T);
        }

        /**
         * 
         */ 
        public T Unmarshal<T>(Stream input)
        {
            // TODO: GG-8535: Implement.
            return default(T);
        }
        
        /**
         * <summary>Context.</summary>
         */ 
        private class Context 
        {
            /** Per-connection serialization context. */
            private readonly GridClientPortableSerializationContext ctx;

            /** Output. */
            private readonly Stream stream;

            /** Tracking wrier. */
            private readonly Writer writer;
            
            /**
             * <summary>Constructor.</summary>
             * <param name="ctx">Client connection context.</param>
             * <param name="output">Output.</param>
             */
            public Context(GridClientPortableSerializationContext ctx, Stream stream)
            {
                this.ctx = ctx;
                this.stream = stream;

                writer = new Writer(this);
            }

            /** Object preventing direct write to the stream. */
            private object blocker;

            private readonly ICollection<Action> actions = new List<Action>();

            private int curHndNum;

            private readonly IDictionary<GridClientPortableObjectHandle, int> hnds = new Dictionary<GridClientPortableObjectHandle, int>();

            /**
             * <summary>Write object to the context.</summary>
             * <param name="obj">Object.</param>
             * <returns>Length of written data.</returns>
             */ 
            public int Write(object obj)
            {
                if (obj == null)
                {
                    // Special case for null value.
                    if (blocker == null)
                        stream.WriteByte(HDR_NULL);
                    else
                        actions.Add(() => { stream.WriteByte(HDR_NULL); });

                    return 1;
                }
                else
                {
                    Type type = obj.GetType();

                    // 1. Primitive?
                    int typeId = GridClientPortableUilts.PrimitiveTypeId(obj.GetType());

                    if (typeId != 0)
                    {
                        // Writing primitive value.
                        if (blocker == null)
                        {
                            stream.WriteByte(HDR_FULL);

                            GridClientPortableUilts.WriteInt(typeId, stream);
                            GridClientPortableUilts.WritePrimitive(typeId, obj, stream);
                        }
                        else
                        {
                            actions.Add(() => 
                            {
                                stream.WriteByte(HDR_FULL);

                                GridClientPortableUilts.WriteInt(typeId, stream);
                                GridClientPortableUilts.WritePrimitive(typeId, obj, stream);
                            });

                            return 5 + GridClientPortableUilts.PrimitiveLength(typeId);
                        }
                    }

                    // Dealing with handles.
                    GridClientPortableObjectHandle hnd = new GridClientPortableObjectHandle(obj);

                    int hndNum = hnds[hnd];
                    
                    if (hndNum == null)
                        hnds.Add(hnd, curHndNum++);
                    else 
                    {
                        if (blocker == null)
                        {
                            // We can be here in case of String, UUID or primitive array.
                            stream.WriteByte(HDR_HND);

                            GridClientPortableUilts.WriteInt(hndNum, stream);
                        }
                        else 
                        {
                            actions.Add(() => 
                            {
                                stream.WriteByte(HDR_HND);

                                GridClientPortableUilts.WriteInt(hndNum, stream);
                            });

                        }

                        return 5;
                    }

                    // 2. String?
                    if (type == typeof(string))
                    {
                        if (blocker == null)
                        {

                        }
                        else
                        {

                        }

                        return 0; // TODO: GG-8535: Implement.
                    }

                    // 3. GUID?
                    // TODO: GG-8535: Implement.

                    // 4. Primitive array?


                    /** Dealing with complex object. */
                    
                    // 5. Object array?

                    // 6. Collection?

                    // 7. Map?

                    // 8. Just object.
                    

                }
            }
        }

        /**
         * <summary>Writer.</summary>
         */ 
        private class Writer //: IGridClientPortableWriter 
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
