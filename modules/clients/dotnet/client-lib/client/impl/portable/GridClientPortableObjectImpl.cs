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
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.IO;
    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;
    
    /**
     * <summary>Portable object implementation.</summary>
     */ 
    internal class GridClientPortableObjectImpl : IGridClientPortableObject
    {
        /** Empty fields collection. */
        private static readonly IDictionary<int, int> EMPTY_FIELDS = 
            new GridClientPortableReadOnlyDictionary<int, int>(new Dictionary<int, int>());

        /** Marshaller. */
        private readonly GridClientPortableMarshaller marsh;
        
        /** User type. */
        private readonly bool userType;

        /** Type ID. */
        private readonly int typeId;

        /** Hash code. */
        private readonly int hashCode;

        /** Raw data of this portable object. */
        private readonly byte[] data;

        /** Offset in data array. */
        private readonly int offset;

        /** Data length. */
        private readonly int len;

        /** Raw data offset. */
        private readonly int rawDataOffset;

        /** Fields. */
        private readonly IDictionary<int, int> fields;

        /**
         * <summary>Constructor.</summary>
         * <param name="marsh">Marshaller.</param>
         * <param name="data">Data bytes.</param>
         * <param name="offset">Offset.</param>
         * <param name="len">Length.</param>
         * <param name="userType">User type flag.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="hashCode">Hash code.</param>
         * <param name="rawDataOffset">Raw data offset.</param>
         * <param name="fields">Fields.</param>
         */
        public GridClientPortableObjectImpl(GridClientPortableMarshaller marsh, byte[] data, int offset,
            int len, bool userType, int typeId, int hashCode, int rawDataOffset, IDictionary<int, int> fields)
        {
            this.marsh = marsh;

            this.data = data;
            this.offset = offset;
            this.len = len;

            this.userType = userType;
            this.typeId = typeId;
            this.hashCode = hashCode;            
            this.rawDataOffset = rawDataOffset;

            this.fields = fields == null ? EMPTY_FIELDS : new GridClientPortableReadOnlyDictionary<int, int>(fields);
        }

        /** <inheritdoc /> */
        public int HashCode()
        {
            return hashCode;
        }

        /** <inheritdoc /> */
        public bool IsUserType()
        {
            return userType;
        }

        /** <inheritdoc /> */
        public int TypeId()
        {
            return typeId;
        }        
       
        /** <inheritdoc /> */
        public F Field<F>(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public T Deserialize<T>()
        {
            return new GridClientPortableReadContext(marsh, marsh.IdToDescriptor, Stream()).Deserialize<T>(this);
        }

        /** <inheritdoc /> */
        public IGridClientPortableObject Copy(IDictionary<string, object> fields)
        {
            throw new System.NotImplementedException();
        }
        
        /**
         * <summary>Offset.</summary>
         */ 
        public int Offset
        {
            get { return offset; }
        }

        /**
         * <summary>Raw data offset.</summary>
         */
        public int RawDataOffset
        {
            get { return rawDataOffset;  }
        }

        /**
         * <summary>Gets portable object data as stream.</summary>
         * <returns>Stream.</returns>
         */ 
        public MemoryStream Stream()
        {
            MemoryStream stream = new MemoryStream(data);

            stream.Position = offset;

            return stream;
        }

        /**
         * <summary>Gets position of the given field ID.</summary>
         * <param name="fieldId">Field ID.</param>
         * <returns>Position.</returns>
         */
        public int? Position(int fieldId)
        {
            int pos;

            return fields.TryGetValue(fieldId, out pos) ? pos : (int?)null;
        }
    }
}
