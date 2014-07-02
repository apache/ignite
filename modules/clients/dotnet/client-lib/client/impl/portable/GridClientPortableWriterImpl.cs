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
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteBoolean(bool val)
        {
            PU.WriteBoolean(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteBooleanArray(string fieldName, bool[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteBooleanArray(bool[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteByte(string fieldName, byte val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteByte(byte val)
        {
            PU.WriteByte(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteByteArray(string fieldName, byte[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteByteArray(byte[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteShort(string fieldName, short val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteShort(short val)
        {
            PU.WriteShort(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteShortArray(string fieldName, short[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteShortArray(short[] val)
        {
            ctx.Write(val);
        }
        
        /** <inheritdoc /> */
        public void WriteChar(string fieldName, char val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteChar(char val)
        {
            PU.WriteShort((short)val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteCharArray(string fieldName, char[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteCharArray(char[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteInt(string fieldName, int val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteInt(int val)
        {
            PU.WriteInt(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteIntArray(string fieldName, int[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteIntArray(int[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteLong(string fieldName, long val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteLong(long val)
        {
            PU.WriteLong(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteLongArray(string fieldName, long[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteLongArray(long[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteFloat(string fieldName, float val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteFloat(float val)
        {
            PU.WriteFloat(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteFloatArray(string fieldName, float[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteFloatArray(float[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteDouble(string fieldName, double val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteDouble(double val)
        {
            PU.WriteDouble(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(string fieldName, double[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(double[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteDate(string fieldName, DateTime? val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteDate(DateTime? val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteDateArray(string fieldName, DateTime?[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteDateArray(DateTime?[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteString(string fieldName, string val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteString(string val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteStringArray(string fieldName, string[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteStringArray(string[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteGuid(string fieldName, Guid? val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteGuid(Guid? val)
        {
            ctx.Write(val); 
        }

        /** <inheritdoc /> */
        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteGuidArray(Guid?[] val)
        {
            ctx.Write(val);
        }
        
        /** <inheritdoc /> */
        public void WriteObject<T>(string fieldName, T val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteObject<T>(T val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteObjectArray<T>(T[] val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteCollection(string fieldName, ICollection val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteCollection(ICollection val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteGenericCollection<T>(string fieldName, ICollection<T> val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteGenericCollection<T>(ICollection<T> val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteDictionary(string fieldName, IDictionary val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteDictionary(IDictionary val)
        {
            ctx.Write(val);
        }

        /** <inheritdoc /> */
        public void WriteGenericDictionary<K, V>(string fieldName, IDictionary<K, V> val)
        {
            WriteUsualField(fieldName, val);
        }

        /** <inheritdoc /> */
        public void WriteGenericDictionary<K, V>(IDictionary<K, V> val)
        {
            ctx.Write(val);
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
         * <summary>Write usual field.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="val">Value.</param>
         */
        private void WriteUsualField(string fieldName, object val)
        {
            long pos = WriteField(fieldName);

            ctx.Write(val);

            WriteLength(pos);
        }

        /**
         * <summary>Write field.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Position for length.</returns>
         */ 
        private long WriteField(string fieldName)
        {
            if (ctx.CurrentRaw)
                throw new GridClientPortableException("Cannot write named fields after raw data is written.");

            int fieldIdRef = ctx.CurrentMapper.FieldId(ctx.CurrentTypeId, fieldName);

            int fieldId = fieldIdRef != 0 ? fieldIdRef : PU.StringHashCode(fieldName.ToLower());

            Console.WriteLine("Write field: " + fieldName + " " + fieldId + " " + ctx.Stream.Position);

            PU.WriteInt(fieldId, ctx.Stream);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            return pos;
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
