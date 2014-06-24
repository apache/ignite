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
            WriteField(fieldName);

            PU.WriteInt(1 + 1, ctx.Stream);
            PU.WriteByte(PU.TYPE_BYTE, ctx.Stream);

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
            WriteField(fieldName);
                
            PU.WriteInt(1 + 1 + (val == null ? 0 : val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_BYTE, ctx.Stream);
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
            WriteField(fieldName);

            PU.WriteInt(1 + 2, ctx.Stream);
            PU.WriteByte(PU.TYPE_SHORT, ctx.Stream);
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
            WriteField(fieldName);

            PU.WriteInt(1 + (val == null ? 0 : 2 * val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_SHORT, ctx.Stream);

            WriteShortArray(val);
        }

        /** <inheritdoc /> */
        public void WriteShortArray(short[] val)
        {
            PU.WriteShortArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteInt(string fieldName, int val)
        {
            WriteField(fieldName);

            PU.WriteInt(1 + 4, ctx.Stream);
            PU.WriteByte(PU.TYPE_INT, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1+ 1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_INT, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1 + 8, ctx.Stream);
            PU.WriteByte(PU.TYPE_LONG, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1 + 1 + (val == null ? 0 : 8 * val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_BYTE, ctx.Stream);

            WriteLongArray(val);
        }

        /** <inheritdoc /> */
        public void WriteLongArray(long[] val)
        {
            PU.WriteLongArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteChar(string fieldName, char val)
        {
            WriteField(fieldName);

            PU.WriteInt(1 + 2, ctx.Stream);
            PU.WriteByte(PU.TYPE_CHAR, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1 + 1 + (val == null ? 0 : 2 * val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_CHAR, ctx.Stream);
                 
            WriteCharArray(val);  
        }

        /** <inheritdoc /> */
        public void WriteCharArray(char[] val)
        {
            PU.WriteCharArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteFloat(string fieldName, float val)
        {
            WriteField(fieldName);

            PU.WriteInt(1 + 4, ctx.Stream);
            PU.WriteByte(PU.TYPE_FLOAT, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1 + 1 + (val == null ? 0 : 4 * val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_FLOAT, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1+ 8, ctx.Stream);
            PU.WriteByte(PU.TYPE_DOUBLE, ctx.Stream);

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
            WriteField(fieldName);

            PU.WriteInt(1 + 1 + (val == null ? 0 : 8 * val.Length), ctx.Stream);
            PU.WriteByte(PU.TYPE_ARRAY_DOUBLE, ctx.Stream);

            WriteDoubleArray(val);
        }

        /** <inheritdoc /> */
        public void WriteDoubleArray(double[] val)
        {
            PU.WriteDoubleArray(val, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteString(string fieldName, string val)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(PU.TYPE_STRING, ctx.Stream);

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
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(PU.TYPE_ARRAY_STRING, ctx.Stream);

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
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            PU.WriteInt(1 + 1 + (val.HasValue ? 16 : 0), ctx.Stream);

            PU.WriteByte(PU.TYPE_GUID, ctx.Stream);

            WriteGuid(val);
        }

        /** <inheritdoc /> */
        public void WriteGuid(Guid? val)
        {
            PU.WriteGuid(val, ctx.Stream); 
        }

        public void WriteGuidArray(string fieldName, Guid?[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteGuidArray(Guid?[] val)
        {
            throw new NotImplementedException();
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

        public void WriteObjectArray<T>(string fieldName, T[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteObjectArray<T>(T[] val)
        {
            throw new NotImplementedException();
        }

        public void WriteCollection(string fieldName, ICollection val)
        {
            throw new NotImplementedException();
        }

        public void WriteCollection(ICollection val)
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

        public void WriteMap(string fieldName, IDictionary val)
        {
            throw new NotImplementedException();
        }

        public void WriteMap(IDictionary val)
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
            
        /**
         * <summary>Mark current output as raw.</summary>
         */ 
        private void MarkRaw()
        {
            if (!ctx.CurrentFrame.Raw)
            {
                ctx.CurrentFrame.RawPosition = ctx.Stream.Position;

                ctx.CurrentFrame.Raw = true;
            }
        }

        /**
         * <summary>Write field.</summary>
         * <param name="fieldName">Field name.</param>
         */ 
        private void WriteField(string fieldName)
        {
            if (ctx.CurrentFrame.Raw)
                throw new GridClientPortableException("Cannot write named fields after raw data is written.");

            // TODO: GG-8535: Check string mode here.
            int? fieldIdRef = ctx.CurrentFrame.Mapper.FieldId(ctx.CurrentFrame.TypeId, fieldName);

            int fieldId = fieldIdRef.HasValue ? fieldIdRef.Value : PU.StringHashCode(fieldName.ToLower());

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
