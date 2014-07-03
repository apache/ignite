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
            return ReadUsualField<bool>(fieldName);
        }

        /** <inheritdoc /> */
        public bool ReadBoolean()
        {
            return PU.ReadBoolean(ctx.Stream);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray(string fieldName)
        {
            return ReadUsualField<bool[]>(fieldName);
        }

        /** <inheritdoc /> */
        public bool[] ReadBooleanArray()
        {
            return ctx.Deserialize<bool[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public byte ReadByte(string fieldName)
        {
            return ReadUsualField<byte>(fieldName);
        }

        /** <inheritdoc /> */
        public byte ReadByte()
        {
            return PU.ReadByte(ctx.Stream);
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray(string fieldName)
        {
            return ReadUsualField<byte[]>(fieldName);
        }

        /** <inheritdoc /> */
        public byte[] ReadByteArray()
        {
            return ctx.Deserialize<byte[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public short ReadShort(string fieldName)
        {
            return ReadUsualField<short>(fieldName);
        }

        /** <inheritdoc /> */
        public short ReadShort()
        {
            return PU.ReadShort(ctx.Stream);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray(string fieldName)
        {
            return ReadUsualField<short[]>(fieldName);
        }

        /** <inheritdoc /> */
        public short[] ReadShortArray()
        {
            return ctx.Deserialize<short[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public char ReadChar(string fieldName)
        {
            return ReadUsualField<char>(fieldName);
        }

        /** <inheritdoc /> */
        public char ReadChar()
        {
            return (char)PU.ReadShort(ctx.Stream);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray(string fieldName)
        {
            return ReadUsualField<char[]>(fieldName);
        }

        /** <inheritdoc /> */
        public char[] ReadCharArray()
        {
            return ctx.Deserialize<char[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public int ReadInt(string fieldName)
        {
            return ReadUsualField<int>(fieldName);
        }

        /** <inheritdoc /> */
        public int ReadInt()
        {
            return PU.ReadInt(ctx.Stream);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray(string fieldName)
        {
            return ReadUsualField<int[]>(fieldName);
        }

        /** <inheritdoc /> */
        public int[] ReadIntArray()
        {
            return ctx.Deserialize<int[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public long ReadLong(string fieldName)
        {
            return ReadUsualField<long>(fieldName);
        }

        /** <inheritdoc /> */
        public long ReadLong()
        {
            return PU.ReadLong(ctx.Stream);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray(string fieldName)
        {
            return ReadUsualField<long[]>(fieldName);
        }

        /** <inheritdoc /> */
        public long[] ReadLongArray()
        {
            return ctx.Deserialize<long[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public float ReadFloat(string fieldName)
        {
            return ReadUsualField<float>(fieldName);
        }

        /** <inheritdoc /> */
        public float ReadFloat()
        {
            return PU.ReadFloat(ctx.Stream);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray(string fieldName)
        {
            return ReadUsualField<float[]>(fieldName);
        }

        /** <inheritdoc /> */
        public float[] ReadFloatArray()
        {
            return ctx.Deserialize<float[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public double ReadDouble(string fieldName)
        {
            return ReadUsualField<double>(fieldName);
        }

        /** <inheritdoc /> */
        public double ReadDouble()
        {
            return PU.ReadDouble(ctx.Stream);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray(string fieldName)
        {
            return ReadUsualField<double[]>(fieldName);
        }

        /** <inheritdoc /> */
        public double[] ReadDoubleArray()
        {
            return ctx.Deserialize<double[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate(string fieldName)
        {
            return ReadUsualField<DateTime?>(fieldName);
        }

        /** <inheritdoc /> */
        public DateTime? ReadDate()
        {
            return ctx.Deserialize<DateTime?>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray(string fieldName)
        {
            return ReadUsualField<DateTime?[]>(fieldName);
        }

        /** <inheritdoc /> */
        public DateTime?[] ReadDateArray()
        {
            return ctx.Deserialize<DateTime?[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public string ReadString(string fieldName)
        {
            return ReadUsualField<string>(fieldName);
        }

        /** <inheritdoc /> */
        public string ReadString()
        {
            return ctx.Deserialize<string>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray(string fieldName)
        {
            return ReadUsualField<string[]>(fieldName);
        }

        /** <inheritdoc /> */
        public string[] ReadStringArray()
        {
            return ctx.Deserialize<string[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid(string fieldName)
        {
            return ReadUsualField<Guid?>(fieldName);
        }

        /** <inheritdoc /> */
        public Guid? ReadGuid()
        {
            return ctx.Deserialize<Guid?>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray(string fieldName)
        {
            return ReadUsualField<Guid?[]>(fieldName);
        }

        /** <inheritdoc /> */
        public Guid?[] ReadGuidArray()
        {
            return ctx.Deserialize<Guid?[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>(string fieldName)
        {
            return ReadUsualField<T>(fieldName);
        }

        /** <inheritdoc /> */
        public T ReadObject<T>()
        {
            return ctx.Deserialize<T>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>(string fieldName)
        {
            return ReadUsualField<T[]>(fieldName);
        }

        /** <inheritdoc /> */
        public T[] ReadObjectArray<T>()
        {
            return ctx.Deserialize<T[]>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName)
        {
            return ReadCollection(fieldName, null, null);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection()
        {
            return ReadCollection(null, null);
        }

        /** <inheritdoc /> */
        public ICollection ReadCollection(string fieldName, GridClientPortableCollectionFactory factory, 
            GridClientPortableCollectionAdder adder)
        {
            PSH.COLLECTION_FACTORY.Value = factory;
            PSH.COLLECTION_ADDER.Value = adder;

            return ReadUsualField<ICollection>(fieldName);
        }
        
        /** <inheritdoc /> */
        public ICollection ReadCollection(GridClientPortableCollectionFactory factory, GridClientPortableCollectionAdder adder)
        {
            PSH.COLLECTION_FACTORY.Value = factory;
            PSH.COLLECTION_ADDER.Value = adder;

            return ctx.Deserialize<ICollection>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName)
        {
            return ReadGenericCollection<T>(fieldName, null);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>()
        {
            return ReadGenericCollection<T>((GridClientPortableGenericCollectionFactory<T>)null);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(string fieldName, GridClientPortableGenericCollectionFactory<T> factory)
        {
            PSH.GENERIC_COLLECTION_FACTORY.Value = factory;

            return ReadUsualField<ICollection<T>>(fieldName);
        }

        /** <inheritdoc /> */
        public ICollection<T> ReadGenericCollection<T>(GridClientPortableGenericCollectionFactory<T> factory)
        {
            PSH.GENERIC_COLLECTION_FACTORY.Value = factory;

            return ctx.Deserialize<ICollection<T>>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName)
        {
            return ReadDictionary(fieldName, null);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary()
        {
            return ReadDictionary((GridClientPortableDictionaryFactory)null);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(string fieldName, GridClientPortableDictionaryFactory factory)
        {
            PSH.DICTIONARY_FACTORY.Value = factory;

            return ReadUsualField<IDictionary>(fieldName);
        }

        /** <inheritdoc /> */
        public IDictionary ReadDictionary(GridClientPortableDictionaryFactory factory)
        {
            PSH.DICTIONARY_FACTORY.Value = factory;

            return ctx.Deserialize<IDictionary>(ctx.Stream);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(string fieldName)
        {
            return ReadGenericDictionary<K, V>(fieldName, null);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>()
        {
            return ReadGenericDictionary<K, V>((GridClientPortableGenericDictionaryFactory<K, V>)null);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(string fieldName, GridClientPortableGenericDictionaryFactory<K, V> factory)
        {
            PSH.GENERIC_DICTIONARY_FACTORY.Value = factory;

            return ReadUsualField<IDictionary<K, V>>(fieldName);
        }

        /** <inheritdoc /> */
        public IDictionary<K, V> ReadGenericDictionary<K, V>(GridClientPortableGenericDictionaryFactory<K, V> factory)
        {
            PSH.GENERIC_DICTIONARY_FACTORY.Value = factory;

            return ctx.Deserialize<IDictionary<K, V>>(ctx.Stream);
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
                return PU.ReadPortable0(ctx.Stream, ctx.Marshaller, doDetach);
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

            ctx.Stream.Seek(4, SeekOrigin.Current);
        }

        /**
         * <summary>Read usual field.</summary>
         * <param name="fieldName">Field name.</param>
         * <returns>Value.</returns>
         */
        public T ReadUsualField<T>(string fieldName)
        {
            Position(fieldName);

            return ctx.Deserialize<T>(ctx.Stream);
        }
    }
}
