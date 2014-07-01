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

        /** Detach flag. */
        private bool detach;

        /** Detach mode flag. */
        private bool detachMode;

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
            PositionAndCheck(fieldName, PU.TYPE_BOOL, "bool");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_BOOL, "bool array");

            return ReadBooleanArray();
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            return PU.ReadBooleanArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_BYTE, "byte");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_BYTE, "byte array");

            return ReadByteArray();
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            return (byte[])PU.ReadByteArray(ctx.Stream, false);
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_SHORT, "short");
            
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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_SHORT, "short array");

            return ReadShortArray();
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            return (short[])PU.ReadShortArray(ctx.Stream, true);
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_CHAR, "char");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_CHAR, "char array");

            return ReadCharArray();
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            return PU.ReadCharArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_INT, "int");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_INT, "int array");

            return ReadIntArray();
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            return (int[])PU.ReadIntArray(ctx.Stream, true);
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_LONG, "long");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_LONG, "long array");

            return ReadLongArray();
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            return (long[])PU.ReadLongArray(ctx.Stream, true);
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_FLOAT, "float");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_FLOAT, "float array");

            return ReadFloatArray();
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            return PU.ReadFloatArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_DOUBLE, "double");
            
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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_DOUBLE, "double array");

            return ReadDoubleArray();
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            return PU.ReadDoubleArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_DATE, "date");

            return ReadDate();
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate()
        {
            return PU.ReadDate(ctx.Stream);
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_DATE, "date array");

            return ReadDateArray();
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray()
        {
            return PU.ReadDateArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_STRING, "string");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_STRING, "string array");

            return ReadStringArray();
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            return PU.ReadStringArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_GUID, "Guid");

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY_GUID, "Guid array");

            return ReadGuidArray();
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            return PU.ReadGuidArray(ctx.Stream);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            Position(fieldName);

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
            PositionAndCheck(fieldName, PU.TYPE_ARRAY, "array");

            return ReadObjectArray<T>();
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>()
        {
            return (T[])PU.ReadArray(ctx);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_COLLECTION, "collection");

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
            PositionAndCheck(fieldName, PU.TYPE_COLLECTION, "collection");

            return ReadCollection(factory, adder);
        }
        
        /** <inheritdoc /> */
        public ICollection ReadCollection(GridClientPortableCollectionFactory factory, GridClientPortableCollectionAdder adder)
        {
            return PU.ReadCollection(ctx, factory, adder);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_COLLECTION, "collection");

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
            PositionAndCheck(fieldName, PU.TYPE_COLLECTION, "collection");

            return ReadGenericCollection(factory);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(GridClientPortableGenericCollectionFactory<T> factory)
        {
            return PU.ReadGenericCollection<T>(ctx, factory);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_DICTIONARY, "dictionary");

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
            PositionAndCheck(fieldName, PU.TYPE_DICTIONARY, "dictionary");

            return ReadDictionary(factory);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(GridClientPortableDictionaryFactory factory)
        {
            return PU.ReadDictionary(ctx, factory);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(string fieldName)
        {
            PositionAndCheck(fieldName, PU.TYPE_DICTIONARY, "dictionary");

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
            PositionAndCheck(fieldName, PU.TYPE_DICTIONARY, "dictionary");

            return ReadGenericDictionary<K, V>(factory);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(GridClientPortableGenericDictionaryFactory<K, V> factory)
        {
            return PU.ReadGenericDictionary<K, V>(ctx, factory);
        }

        /**
         * <summary>Read portable object.</summary>
         */ 
        public IGridClientPortableObject ReadPortable()
        {
            bool doDetach = false;

            if (detach)
            {
                detach = false; // Reset detach flag so that inner objects will not pick it.
                detachMode = true; // Set detach mode so that nobody can set detach flag again.
                doDetach = true; // Local variable to perform detach.
            }

            try
            {
                return PU.ReadPortable(ctx.Stream, ctx.Marshaller, doDetach);
            }
            finally
            {
                if (detachMode)
                    detachMode = false;
            }
        }

        /**
         * <summary>Enable detach mode for the next object write.</summary>
         */ 
        public void DetachNext()
        {
            if (!detachMode)
                detach = true;
        }

        /**
         * <summary>Mark current output as raw.</summary>
         */
        private void MarkRaw()
        {
            if (!ctx.CurrentRaw)
            {
                ctx.CurrentRaw = true;

                int rawDataOffset = ctx.CurrentPortable.RawDataOffset;

                if (rawDataOffset == -1)
                    throw new GridClientPortableException("Object doesn't contain raw data [typeId=" +
                        ctx.CurrentPortable.TypeId() + ']');

                ctx.Stream.Seek(ctx.CurrentPortable.Offset + rawDataOffset, SeekOrigin.Begin);
            }            
        }

        /**
         * <summary>Position stream before field data and check it's type.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="typeId">Expected type ID.</param>
         * <param name="typeName">Type name</param>
         */
        private void PositionAndCheck(string fieldName, byte typeId, string typeName)
        {
            Position(fieldName);

            ctx.Stream.Seek(4, SeekOrigin.Current);

            byte hdr = (byte)ctx.Stream.ReadByte();

            if (hdr != typeId)
                throw new GridClientPortableInvalidFieldException("Written field is not " + typeName + ": " + fieldName);
        }

        /**
         * <summary>Position stream right after field ID.</summary>
         * <param name="fieldName">Field name.</param>
         */ 
        private void Position(string fieldName)
        {
            if (ctx.CurrentRaw)
                throw new GridClientPortableException("Cannot read named fields after raw data is read.");

            int fieldIdRef = ctx.CurrentMapper.FieldId(ctx.CurrentTypeId, fieldName);

            int fieldId = fieldIdRef != 0 ? fieldIdRef : PU.StringHashCode(fieldName.ToLower());

            int? fieldPosRef = ctx.CurrentPortable.Position(fieldId);

            if (fieldPosRef.HasValue)
                ctx.Stream.Seek(ctx.CurrentPortable.Offset + fieldPosRef.Value + 4, SeekOrigin.Begin);
            else
                throw new GridClientPortableInvalidFieldException("Cannot find field in portable object [typeId=" +
                    ctx.CurrentPortable.TypeId() + ", field=" + fieldName + ']');
        }
    }
}
