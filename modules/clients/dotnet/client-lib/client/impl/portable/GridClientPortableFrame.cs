/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Portable
{
    using GridGain.Client.Portable;

    /**
     * <summary>Serialization frame.</summary>
     */
    internal class GridClientPortableFrame
    {
        /**
         * <summary>Constructor.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="mapper">Field mapper.</param>
         * <param name="portable">Underlying portable object.</param>
         */
        public GridClientPortableFrame(int typeId, GridClientPortableIdResolver mapper, 
            GridClientPortableObjectImpl portable)
        {
            TypeId = typeId;
            Mapper = mapper;
            Portable = portable;
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
         * <summary>ID mapper.</summary>
         */
        public GridClientPortableIdResolver Mapper
        {
            get;
            private set;
        }

        /**
         * <summary>Portable object.</summary>
         */
        public GridClientPortableObjectImpl Portable
        {
            get;
            private set;
        }

        /**
         * <summary>Raw mode.</summary>
         */
        public bool Raw
        {
            get;
            set;
        }

        /**
         * <summary>Raw position.</summary>
         */
        public long RawPosition
        {
            get;
            set;
        }
    }
}
