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
     * <summary>Portable ID attribute. When applied to class defines class ID, when 
     * applied to field defines field ID.</summary>
     */
    [AttributeUsage(AttributeTargets.Field)]
    public class GridClientPortableId : Attribute
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="id">Class ID.</param>
         */
        public GridClientPortableId(int id)
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
