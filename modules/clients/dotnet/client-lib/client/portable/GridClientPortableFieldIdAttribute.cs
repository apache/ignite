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
     * <summary>Portable class field ID.</summary>
     */
    [AttributeUsage(AttributeTargets.Field)]
    class GridClientPortableFieldIdAttribute : Attribute
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="id">Class field ID.</param>
         */ 
        public GridClientPortableFieldIdAttribute(int id)
        {
            Id = id;
        }

        /**
         * <summary>Class field ID.</summary>
         */ 
        public int Id
        {
            get;
            private set;
        }
    }
}
