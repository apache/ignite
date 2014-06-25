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

        /** <inheritdoc /> */
        public void WriteCollection(string fieldName, ICollection val)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(PU.TYPE_COLLECTION, ctx.Stream);

            WriteCollection(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteCollection(ICollection val)
        {
            if (val != null)
            {
                PU.WriteInt(val.Count, ctx.Stream);

                PU.WriteByte(val.GetType() == typeof(ArrayList) ? PU.COLLECTION_ARRAY_LIST : PU.COLLECTION_CUSTOM, ctx.Stream);

                foreach (Object elem in val)
                    ctx.Write(elem);
            }
            else
                PU.WriteInt(-1, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteGenericCollection<T>(string fieldName, ICollection<T> val)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(PU.TYPE_COLLECTION, ctx.Stream);

            WriteGenericCollection<T>(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteGenericCollection<T>(ICollection<T> val)
        {
            if (val != null)
            {
                PU.WriteInt(val.Count, ctx.Stream);

                Type type = val.GetType().GetGenericTypeDefinition();

                byte colType;

                if (type == typeof(List<>))
                    colType = PU.COLLECTION_ARRAY_LIST;
                else if (type == typeof(LinkedList<>))
                    colType = PU.COLLECTION_LINKED_LIST;
                else if (type == typeof(HashSet<>))
                    colType = PU.COLLECTION_HASH_SET;
                else if (type == typeof(SortedSet<>))
                    colType = PU.COLLECTION_SORTED_SET;
                else 
                    colType = PU.COLLECTION_CUSTOM;

                PU.WriteByte(colType, ctx.Stream);

                foreach (T elem in val)
                    ctx.Write(elem);
            }
            else
                PU.WriteInt(-1, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteDictionary(string fieldName, IDictionary val)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(PU.TYPE_MAP, ctx.Stream);

            WriteDictionary(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteDictionary(IDictionary val)
        {
            if (val != null)
            {
                PU.WriteInt(val.Count, ctx.Stream);

                PU.WriteByte(val.GetType() == typeof(Hashtable) ? PU.MAP_HASH_MAP : PU.MAP_CUSTOM, ctx.Stream);

                foreach (DictionaryEntry entry in val)
                {
                    ctx.Write(entry.Key);
                    ctx.Write(entry.Value);
                }
            }
            else
                PU.WriteInt(-1, ctx.Stream);
        }

        /** <inheritdoc /> */
        public void WriteGenericDictionary<K, V>(string fieldName, IDictionary<K, V> val)
        {
            WriteField(fieldName);

            long pos = ctx.Stream.Position;

            ctx.Stream.Seek(4, SeekOrigin.Current);

            PU.WriteByte(PU.TYPE_MAP, ctx.Stream);

            WriteGenericDictionary(val);

            WriteLength(pos);
        }

        /** <inheritdoc /> */
        public void WriteGenericDictionary<K, V>(IDictionary<K, V> val)
        {
            if (val != null)
            {
                PU.WriteInt(val.Count, ctx.Stream);

                Type type = val.GetType().GetGenericTypeDefinition();

                byte dictType;

                if (type == typeof(Dictionary<,>))
                    dictType = PU.MAP_HASH_MAP;
                else if (type == typeof(SortedDictionary<,>))
                    dictType = PU.MAP_SORTED_MAP;
                else if (type == typeof(ConcurrentDictionary<,>))
                    dictType = PU.MAP_CONCURRENT_HASH_MAP;
                else
                    dictType = PU.MAP_CUSTOM;

                PU.WriteByte(dictType, ctx.Stream);

                foreach (KeyValuePair<K, V> entry in val)
                {
                    ctx.Write(entry.Key);
                    ctx.Write(entry.Value);
                }
            }
            else
                PU.WriteInt(-1, ctx.Stream);
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
