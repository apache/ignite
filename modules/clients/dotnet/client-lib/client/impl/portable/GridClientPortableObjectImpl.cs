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
    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;
    
    /**
     * <summary>Portable object implementation.</summary>
     */ 
    internal class GridClientPortableObjectImpl : IGridClientPortableObject
    {
        /** Empty fields collection. */
        private static readonly ISet<int> EMPTY_FIELDS = new GridClientPortableReadOnlySet<int>(new HashSet<int>());

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
        private readonly ISet<int> fields;

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
            int len, bool userType, int typeId, int hashCode, int rawDataOffset, ISet<int> fields)
        {
            this.marsh = marsh;

            this.data = data;
            this.offset = offset;
            this.len = len;

            this.userType = userType;
            this.typeId = typeId;
            this.hashCode = hashCode;            
            this.rawDataOffset = rawDataOffset;

            this.fields = fields == null ? EMPTY_FIELDS : new GridClientPortableReadOnlySet<int>(fields);
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
        public string TypeName()
        {
            return marsh.TypeName(typeId, userType);
        }

        /** <inheritdoc /> */
        public ICollection<int> Fields()
        {
            return fields;
        }
        
        /** <inheritdoc /> */
        public int FieldTypeId(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public int FieldTypeName(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public F Field<F>(string fieldName)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public T Deserialize<T>()
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public IGridClientPortableObject Copy(IDictionary<string, object> fields)
        {
            throw new System.NotImplementedException();
        }
    }
}
