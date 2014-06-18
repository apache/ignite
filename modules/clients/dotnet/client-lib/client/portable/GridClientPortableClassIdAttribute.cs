/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.client.portable
{
    using System;

    /**
     * <summary>Portable class ID.</summary>
     */
    [AttributeUsage(AttributeTargets.Field)]
    class GridClientPortableClassIdAttribute : Attribute
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="id">Class ID.</param>
         */ 
        public GridClientPortableClassIdAttribute(int id)
        {
            Id = id;
        }

        /**
         * <summary>Class ID.</summary>
         */ 
        public int Id
        {
            get;
            private set;
        }
    }
}
