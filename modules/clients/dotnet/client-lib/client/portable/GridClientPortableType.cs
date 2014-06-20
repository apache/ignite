/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable
{
    using System;

    /**
     * <summary>Portable type.</summary>
     */
    [AttributeUsage(AttributeTargets.Class)]
    class GridClientPortableType : Attribute
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="idMapperCls">ID mapper class name.</param>
         * <param name="serializerCls">Serializer class name.</param>
         */ 
        public GridClientPortableType(string idMapperCls, string serializerCls)
        {
            IdMapperClass = idMapperCls;
            SerializerClass = serializerCls;
        }

        /**
         * <summary>ID mapper class.</summary>
         */
        public string IdMapperClass
        {
            get;
            private set;
        }

        /**
         * <summary>Serializer class.</summary>
         */
        public string SerializerClass
        {
            get;
            private set;
        }
    }
}
