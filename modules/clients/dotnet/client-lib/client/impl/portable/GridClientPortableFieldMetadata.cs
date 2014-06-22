/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    /**
     * <summary>Field metadata.</summary>
     */ 
    internal class GridClientPortableFieldMetadata
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="fieldName">Field name.</param>
         * <param name="typeId">Type ID.</param>
         * <param name="typeName">Type name.</param>
         */
        public GridClientPortableFieldMetadata(string fieldName, int typeId, string typeName)
        {
            FieldName = fieldName;
            TypeId = typeId;
            TypeName = typeName;
        }

        /**
         * <summary>Field name.</summary>
         */ 
        public string FieldName
        {
            get;
            private set;
        }

        /**
         * <summary>Type ID.</summary>
         */
        public int TypeId
        {
            get;
            private set;
        }

        /**
         * <summary>Type name.</summary>
         */
        public string TypeName
        {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;

            if (obj != null && obj is GridClientPortableFieldMetadata)
            {
                GridClientPortableFieldMetadata that = (GridClientPortableFieldMetadata)obj;

                return FieldName.Equals(that.FieldName) && TypeId.Equals(that.TypeId) && TypeName.Equals(that.TypeName);
            }
            else
                return false;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return FieldName.GetHashCode() + 31 * TypeId + 31 * 31 * TypeName.GetHashCode();
        }
    }
}
