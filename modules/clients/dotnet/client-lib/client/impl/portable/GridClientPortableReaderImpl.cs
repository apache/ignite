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
    using PSH = GridGain.Client.Impl.Portable.GridClientPortableSystemHandlers;

    /**
     * <summary>Portable reader implementation.</summary>
     */ 
    internal class GridClientPortableReaderImpl : IGridClientPortableReader, IGridClientPortableRawReader
    {
        /** Read context. */
        private readonly GridClientPortableReadContext ctx;

        /**
         * <summary>Constructor.</summary>
         * <param name="ctx">Read context.</param>
         */ 
        public GridClientPortableReaderImpl(GridClientPortableReadContext ctx)
        {
            this.ctx = ctx;
        }  

        /** <inheritdoc /> */
        public IGridClientPortableRawReader RawReader()
        {
            MarkRaw();

            return this;
        }

        /** <inheritdoc /> */
        public bool ReadBoolean(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_BOOL)
                throw new GridClientPortableInvalidFieldException("Written field is not bool: " + fieldName);

            return ReadBoolean();
        }

        /** <inheritdoc /> */
        public bool ReadBoolean()
        {
            return PU.ReadBoolean(ctx.Stream);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_BYTE)
                throw new GridClientPortableInvalidFieldException("Written field is not byte: " + fieldName);

            return ReadByte();
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            return (byte)ctx.Stream.ReadByte();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_SHORT)
                throw new GridClientPortableInvalidFieldException("Written field is not short: " + fieldName);

            return ReadShort();
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            return PU.ReadShort(ctx.Stream);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_CHAR)
                throw new GridClientPortableInvalidFieldException("Written field is not char: " + fieldName);

            return ReadChar();
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            return (char)PU.ReadShort(ctx.Stream);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_INT)
                throw new GridClientPortableInvalidFieldException("Written field is not int: " + fieldName);

            return ReadInt();
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            return PU.ReadInt(ctx.Stream);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_LONG)
                throw new GridClientPortableInvalidFieldException("Written field is not long: " + fieldName);

            return ReadLong();
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            return PU.ReadLong(ctx.Stream);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_FLOAT)
                throw new GridClientPortableInvalidFieldException("Written field is not float: " + fieldName);

            return ReadFloat();
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            return PU.ReadFloat(ctx.Stream);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_DOUBLE)
                throw new GridClientPortableInvalidFieldException("Written field is not double: " + fieldName);
            
            return ReadDouble();
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            return PU.ReadDouble(ctx.Stream);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_STRING)
                throw new GridClientPortableInvalidFieldException("Written field is not string: " + fieldName);

            return ReadString();
        }

        /** <inheritdoc /> */
        public string ReadString()
        {
            return PU.ReadString(ctx.Stream);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_GUID)
                throw new GridClientPortableInvalidFieldException("Written field is not Guid: " + fieldName);

            return ReadGuid();
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid()
        {
            return PU.ReadGuid(ctx.Stream);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            return ReadObject<T>();
        }

        /** <inheritdoc /> */
        public T ReadObject<T>()
        {
            return ctx.Deserialize<T>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_MAP)
                throw new GridClientPortableInvalidFieldException("Written field is not dictionary: " + fieldName);

            return ReadDictionary();
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary()
        {
            return ReadDictionary(PSH.CreateHashtable);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName, GridClientPortableDictionaryFactory factory)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_MAP)
                throw new GridClientPortableInvalidFieldException("Written field is not dictionary: " + fieldName);

            return ReadDictionary(factory);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(GridClientPortableDictionaryFactory factory)
        {
            int len = PU.ReadInt(ctx.Stream);

            if (len >= 0)
            {
                // Doesn't support anything in non-generic mode.
                ctx.Stream.Seek(1, SeekOrigin.Current);

                IDictionary res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                {
                    object key = ctx.Deserialize<object>(ctx.Stream);
                    object val = ctx.Deserialize<object>(ctx.Stream);

                    res[key] = val;
                }                    

                return res;
            }
            else
                return null;
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_MAP)
                throw new GridClientPortableInvalidFieldException("Written field is not dictionary: " + fieldName);

            return ReadGenericDictionary<K, V>();
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>()
        {
            return ReadGenericDictionary<K, V>((GridClientPortableGenericDictionaryFactory<K, V>)null);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(string fieldName, GridClientPortableGenericDictionaryFactory<K, V> factory)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_MAP)
                throw new GridClientPortableInvalidFieldException("Written field is not dictionary: " + fieldName);

            return ReadGenericDictionary<K, V>(factory);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(GridClientPortableGenericDictionaryFactory<K, V> factory)
        {
            int len = PU.ReadInt(ctx.Stream);

            if (len >= 0)
            {
                byte colType = PU.ReadByte(ctx.Stream);

                if (factory == null)
                {
                    // Need to detect factory automatically.
                    if (colType == PU.MAP_SORTED_MAP)
                        factory = PSH.CreateSortedDictionary<K, V>;
                    else if (colType == PU.MAP_CONCURRENT_HASH_MAP)
                        factory = PSH.CreateConcurrentDictionary<K, V>;
                    else
                        factory = PSH.CreateDictionary<K, V>;
                }

                IDictionary<K, V> res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                {
                    K key = ctx.Deserialize<K>(ctx.Stream);
                    V val = ctx.Deserialize<V>(ctx.Stream);

                    res[key] = val;
                }

                return res;
            }
            else
                return null;
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_COLLECTION)
                throw new GridClientPortableInvalidFieldException("Written field is not collection: " + fieldName);

            return ReadCollection();
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection()
        {
            return ReadCollection(PSH.CreateArrayList, PSH.AddToArrayList);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName, GridClientPortableCollectionFactory factory, 
            GridClientPortableCollectionAdder adder)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_COLLECTION)
                throw new GridClientPortableInvalidFieldException("Written field is not collection: " + fieldName);

            return ReadCollection(factory, adder);
        }
        
        /** <inheritdoc /> */
        public ICollection ReadCollection(GridClientPortableCollectionFactory factory, GridClientPortableCollectionAdder adder)
        {
            int len = PU.ReadInt(ctx.Stream);

            if (len >= 0)
            {
                // Doesn't support anything in non-generic mode.
                ctx.Stream.Seek(1, SeekOrigin.Current);

                ICollection res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                    adder.Invoke(res, ctx.Deserialize<object>(ctx.Stream));

                return res;
            }
            else
                return null;
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_COLLECTION)
                throw new GridClientPortableInvalidFieldException("Written field is not collection: " + fieldName);

            return ReadGenericCollection<T>();
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>()
        {
            return ReadGenericCollection<T>((GridClientPortableGenericCollectionFactory<T>)null);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName, GridClientPortableGenericCollectionFactory<T> factory)
        {
            PositionField(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != PU.TYPE_COLLECTION)
                throw new GridClientPortableInvalidFieldException("Written field is not collection: " + fieldName);

            return ReadGenericCollection(factory);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(GridClientPortableGenericCollectionFactory<T> factory)
        {
            int len = PU.ReadInt(ctx.Stream);

            if (len >= 0)
            {
                byte colType = PU.ReadByte(ctx.Stream);

                if (factory == null)
                {
                    // Need to detect factory automatically.
                    if (colType == PU.COLLECTION_LINKED_LIST)
                        factory = PSH.CreateLinkedList<T>;
                    else if (colType == PU.COLLECTION_HASH_SET)
                        factory = PSH.CreateHashSet<T>;
                    else if (colType == PU.COLLECTION_SORTED_SET)
                        factory = PSH.CreateHashSet<T>;
                    else 
                        factory = PSH.CreateList<T>;
                }

                ICollection<T> res = factory.Invoke(len);

                for (int i = 0; i < len; i++)
                    res.Add(ctx.Deserialize<T>(ctx.Stream));

                return res;
            }
            else
                return null;
        }
        
        /**
         * <summary>Mark current output as raw.</summary>
         */
        private void MarkRaw()
        {
            if (!ctx.CurrentFrame.Raw)
            {
                ctx.CurrentFrame.Raw = true;

                int rawDataOffset = ctx.CurrentFrame.Portable.RawDataOffset;

                if (rawDataOffset == 0)
                    throw new GridClientPortableException("Object doesn't contain raw data [typeId=" + 
                        ctx.CurrentFrame.Portable.TypeId() + ']');

                ctx.Stream.Seek(ctx.CurrentFrame.Portable.Offset + rawDataOffset, SeekOrigin.Begin);
            }            
        }

        /**
         * <summary>Position stream right after field ID.</summary>
         * <param name="fieldName">Field name.</param>
         */ 
        private void PositionField(string fieldName)
        {
            if (ctx.CurrentFrame.Raw)
                throw new GridClientPortableException("Cannot read named fields after raw data is read.");

            int? fieldIdRef = ctx.CurrentFrame.Mapper.FieldId(ctx.CurrentFrame.TypeId, fieldName);

            int fieldId = fieldIdRef.HasValue ? fieldIdRef.Value : PU.StringHashCode(fieldName.ToLower());

            int? fieldPosRef = ctx.CurrentFrame.Portable.Position(fieldId);

            if (fieldPosRef.HasValue)
                ctx.Stream.Seek(ctx.CurrentFrame.Portable.Offset + fieldPosRef.Value + 4, SeekOrigin.Begin);
            else
                throw new GridClientPortableInvalidFieldException("Cannot find field in portable object [typeId=" +
                    ctx.CurrentFrame.Portable.TypeId() + ", field=" + fieldName + ']');
        }
    }
}
