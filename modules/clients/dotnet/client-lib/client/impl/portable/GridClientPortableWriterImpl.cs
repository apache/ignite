// @csharp.file.header

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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using GridGain.Client.Portable;
    
    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /**
     * <summary>Portable writer implementation.</summary>
     */ 
    internal class GridClientPortableWriterImpl : IGridClientPortableWriter, IGridClientPortableRawWriter
    {
        /** Write context. */
        private readonly GridClientPortableWriteContext ctx;

        /**
            * <summary>Constructor.</summary>
            * <param name="ctx">Context.</param>
            */
        public GridClientPortableWriterImpl(GridClientPortableWriteContext ctx)
        {
            this.ctx = ctx;
        }

        /** <inheritdoc /> */
        public IGridClientPortableRawWriter RawWriter()
        {
            MarkRaw();

            return this;
        }

        /** <inheritdoc /> */
        public void WriteBoolean(string fieldName, bool val)
        {
            WriteField(fieldName);

            PU.WriteInt(1 + 1, ctx.Stream);
            PU.WriteByte(PU.TYPE_BOOL, ctx.Stream);

            WriteBoolean(val);
        }

        /** <inheritdoc /> */
        public void WriteBoolean(bool val)
        {
            PU.WriteBoolean(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            WriteField(fieldName);

            PU.WriteInt(1 + 1 + (val == null ? 0 : val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_BOOL, ctx.Stream);

            WriteBooleanArray(val);
        }

        /** <inheritdoc /> */
        public void WriteBooleanArray(bool[] val)
        {
            PU.WriteBooleanArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteByte(string fieldName, byte val)
        {
            WriteField(fieldName, PU.TYPE_BYTE, 1);

            WriteByte(val);
        }

        /** <inheritdoc /> */
        public void WriteByte(byte val)
        {
            ctx.Stream.WriteByte(val);
        }

        /** <inheritdoc /> */
        public void WriteByteArray(string fieldName, byte[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_BYTE, 4 + (val == null ? 0 : val.Length));

            WriteByteArray(val);
        }

        /** <inheritdoc /> */
        public void WriteByteArray(byte[] val)
        {
            PU.WriteByteArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteShort(string fieldName, short val)
        {
            WriteField(fieldName, PU.TYPE_SHORT, 2);

            WriteShort(val);
        }

        /** <inheritdoc /> */
        public void WriteShort(short val)
        {
            PU.WriteShort(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteShortArray(string fieldName, short[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_SHORT, 4 + (val == null ? 0 : 2 * val.Length));

            WriteShortArray(val);
        }

        /** <inheritdoc /> */
        public void WriteShortArray(short[] val)
        {
            PU.WriteShortArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteChar(string fieldName, char val)
        {
            WriteField(fieldName, PU.TYPE_CHAR, 2);

            WriteShort((short)val);
        }

        /** <inheritdoc /> */
        public void WriteChar(char val)
        {
            WriteShort((short)val);
        }

        /** <inheritdoc /> */
        public void WriteCharArray(string fieldName, char[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_CHAR, 4 + (val == null ? 0 : 2 * val.Length));

            WriteCharArray(val);
        }

        /** <inheritdoc /> */
        public void WriteCharArray(char[] val)
        {
            PU.WriteCharArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteInt(string fieldName, int val)
        {
            WriteField(fieldName, PU.TYPE_INT, 4);

            WriteInt(val);
        }

        /** <inheritdoc /> */
        public void WriteInt(int val)
        {
            PU.WriteInt(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteIntArray(string fieldName, int[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_INT, 4 + (val == null ? 0 : 4 * val.Length));

            WriteIntArray(val);
        }

        /** <inheritdoc /> */
        public void WriteIntArray(int[] val)
        {
            PU.WriteIntArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteLong(string fieldName, long val)
        {
            WriteField(fieldName, PU.TYPE_LONG, 8);

            WriteLong(val);
        }

        /** <inheritdoc /> */
        public void WriteLong(long val)
        {
            PU.WriteLong(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteLongArray(string fieldName, long[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_LONG, 4 + (val == null ? 0 : 8 * val.Length));

            WriteLongArray(val);
        }

        /** <inheritdoc /> */
        public void WriteLongArray(long[] val)
        {
            PU.WriteLongArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteFloat(string fieldName, float val)
        {
            WriteField(fieldName, PU.TYPE_FLOAT, 4);

            WriteFloat(val);
        }

        /** <inheritdoc /> */
        public void WriteFloat(float val)
        {
            PU.WriteFloat(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteFloatArray(string fieldName, float[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_FLOAT, 4 + (val == null ? 0 : 4 * val.Length));

            WriteFloatArray(val);
        }

        /** <inheritdoc /> */
        public void WriteFloatArray(float[] val)
        {
            PU.WriteFloatArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteDouble(string fieldName, double val)
        {
            WriteField(fieldName, PU.TYPE_DOUBLE, 8);

            WriteDouble(val);
        }

        /** <inheritdoc /> */
        public void WriteDouble(double val)
        {
            PU.WriteDouble(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(string fieldName, double[] val)
        {
            WriteField(fieldName, PU.TYPE_ARRAY_DOUBLE, 4 + (val == null ? 0 : 8 * val.Length));

            WriteDoubleArray(val);
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(double[] val)
        {
            PU.WriteDoubleArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteDate(string fieldName, DateTime? val)
        {
            WriteField(fieldName, PU.TYPE_DATE, 1 + (val.HasValue ? 8 : 0));

            WriteDate(val);
        }

        /** <inheritdoc /> */
        public void WriteDate(DateTime? val)
        {
            PU.WriteDate(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteDateArray(string fieldName, DateTime?[] val)
        {
            long pos = WriteField(fieldName, PU.TYPE_ARRAY_DATE);

            WriteDateArray(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteDateArray(DateTime?[] val)
        {
            PU.WriteDateArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteString(string fieldName, string val)
        {
            long pos = WriteField(fieldName, PU.TYPE_STRING);

            WriteString(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteString(string val)
        {
            PU.WriteString(val, ctx.Stream); 
        }

        /** <inheritdoc /> */
        public void WriteStringArray(string fieldName, string[] val)
        {
            long pos = WriteField(fieldName, PU.TYPE_ARRAY_STRING);

            WriteStringArray(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteStringArray(string[] val)
        {
            PU.WriteStringArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteGuid(string fieldName, Guid? val)
        {
            WriteField(fieldName, PU.TYPE_GUID, 1 + (val.HasValue ? 16 : 0));

            WriteGuid(val);
        }

        /** <inheritdoc /> */
        public void WriteGuid(Guid? val)
        {
            PU.WriteGuid(val, ctx.Stream); 
        }

        /** <inheritdoc /> */
        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            long pos = WriteField(fieldName, PU.TYPE_ARRAY_GUID);

            WriteGuidArray(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteGuidArray(Guid?[] val)
        {
            PU.WriteGuidArray(val, ctx.Stream);
        }
        
        /** <inheritdoc /> */
        public void WriteObject<T>(string fieldName, T val)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            WriteObject(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteObject<T>(T val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            long pos = WriteField(fieldName, PU.TYPE_ARRAY);

            WriteObjectArray<T>(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteObjectArray<T>(T[] val)
        {
            PU.WriteArray(val, ctx);
        }

        /** <inheritdoc /> */
        public void WriteCollection(string fieldName, ICollection val)
        {
            long pos = WriteField(fieldName, PU.TYPE_COLLECTION);

            WriteCollection(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteCollection(ICollection val)
        {
            PU.WriteCollection(val, ctx);
        }

        /** <inheritdoc /> */
        public void WriteGenericCollection<T>(string fieldName, ICollection<T> val)
        {
            long pos = WriteField(fieldName, PU.TYPE_COLLECTION);

            WriteGenericCollection<T>(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteGenericCollection<T>(ICollection<T> val)
        {
            PU.WriteGenericCollection<T>(val, ctx);
        }

        /** <inheritdoc /> */
        public void WriteDictionary(string fieldName, IDictionary val)
        {
            long pos = WriteField(fieldName, PU.TYPE_DICTIONARY);

            WriteDictionary(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteDictionary(IDictionary val)
        {
            PU.WriteDictionary(val, ctx);
        }

        /** <inheritdoc /> */
        public void WriteGenericDictionary<K, V>(string fieldName, IDictionary<K, V> val)
        {
            long pos = WriteField(fieldName, PU.TYPE_DICTIONARY);

            WriteGenericDictionary(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteGenericDictionary<K, V>(IDictionary<K, V> val)
        {
            PU.WriteGenericDictionary<K, V>(val, ctx);
        }

        /**
         * <summary>Enable detach mode for the next object write.</summary>
         */ 
        public void DetachNext()
        {
            ctx.DetachNext();
        }

        /**
         * <summary>Mark current output as raw.</summary>
         */ 
        private void MarkRaw()
        {
            if (!ctx.CurrentRaw)
            {
                ctx.CurrentRaw = true;
                ctx.CurrentRawPosition = ctx.Stream.Position;
            }
        }

        /**
         * <summary>Write field header with known length.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="type">Field type.</param>
         * <param name="len">Field length.</param>
         */
        private void WriteField(string fieldName, byte type, int len)
        {
            WriteField(fieldName);

            PU.WriteInt(1 + len, ctx.Stream);
            PU.WriteByte(type, ctx.Stream);
        }

        /**
         * <summary>Write field header with unknown length.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="type">Field type.</param>
         * <returns>Position where length is to be written.</returns>
         */
        private long WriteField(string fieldName, byte type)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(type, ctx.Stream);

            return pos;
        }

        /**
         * <summary>Write field.</summary>
         * <param name="fieldName">Field name.</param>
         */ 
        private void WriteField(string fieldName)
        {
            if (ctx.CurrentRaw)
                throw new GridClientPortableException("Cannot write named fields after raw data is written.");

            int fieldIdRef = ctx.CurrentMapper.FieldId(ctx.CurrentTypeId, fieldName);

            int fieldId = fieldIdRef != 0 ? fieldIdRef : PU.StringHashCode(fieldName.ToLower());

            Console.WriteLine("Write field: " + fieldName + " " + fieldId + " " + ctx.Stream.Position);

            PU.WriteInt(fieldId, ctx.Stream);
        }

        /**
         * <summary></summary>
         * <param name="pos">Position where length should reside.</param>
         */
        private void WriteLength(long pos)
        {
            Stream stream = ctx.Stream;

            long retPos = stream.Position;

            stream.Seek(pos, SeekOrigin.Begin);

            PU.WriteInt((int)(retPos - pos - 4), stream);

            stream.Seek(retPos, SeekOrigin.Begin);
        }
    }
}
