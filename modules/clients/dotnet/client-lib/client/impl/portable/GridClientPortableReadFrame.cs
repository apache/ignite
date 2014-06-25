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
     * <summary>Write frame.</summary>
     */
    internal class GridClientPortableReadFrame
    {
        /** Type ID. */
        private int typeId;

        /** Mapper. */
        private GridClientPortableIdResolver mapper;

        /** Portable object. */
        private GridClientPortableObjectImpl portable;

        /** Raw flag. */
        private bool raw;

        /**
         * <summary>Constructor.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="mapper">Field mapper.</param>
         * <param name="portable">Underlying portable object.</param>
         */
        public GridClientPortableReadFrame(int typeId, GridClientPortableIdResolver mapper,
            GridClientPortableObjectImpl portable)
        {
            this.typeId = typeId;
            this.mapper = mapper;
            this.portable = portable;
        }

        /**
         * <summary>Type ID.</summary>
         */
        public int TypeId
        {
            get { return typeId; }
        }

        /**
         * <summary>ID mapper.</summary>
         */
        public GridClientPortableIdResolver Mapper
        {
            get { return mapper; }
        }

        /**
         * <summary>Portable object.</summary>
         */
        public GridClientPortableObjectImpl Portable
        {
            get { return portable; }
        }

        /**
         * <summary>Raw mode.</summary>
         */
        public bool Raw
        {
            get { return raw; }
            set { raw = value; }
        }
    }
}
