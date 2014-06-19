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

    /**
     * <summary>Object header.</summary>
     */ 
    class GridClientPortableHeader
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="type">Type.</param>
         * <param name="fields">Fields.</param>
         */
        public GridClientPortableHeader(Type type, List<String> fields)
        {
            Type = type;
            Fields = fields;
        }

        /**
         * <summary>Type.</summary>
         */
        public Type Type
        {
            get;
            private set;
        }

        /**
         * <summary>Fields.</summary>
         */ 
        public List<string> Fields
        {
            get;
            private set;
        }

        /** <inheritdoc /> */
        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;

            if (obj != null && obj is GridClientPortableHeader)
            {
                GridClientPortableHeader that = (GridClientPortableHeader)obj;

                return Type.Equals(that.Type) && (Fields == null && that.Fields == null || Fields != null && Fields.Equals(that.Fields));
            }
            else
                return false;
        }

        /** <inheritdoc /> */
        public override int GetHashCode()
        {
            return Type.GetHashCode() + (Fields == null ? 0 : 31 * Fields.GetHashCode());
        }
    }
}
