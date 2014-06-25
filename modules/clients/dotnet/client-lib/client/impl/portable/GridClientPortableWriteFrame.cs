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
    internal class GridClientPortableWriteFrame
    {
        /** Type ID. */
        private int typeId;

        /** Mapper. */
        private GridClientPortableIdResolver mapper;

        /** Raw flag. */
        private bool raw;

        /** Raw position. */
        private long rawPos;

        /**
         * <summary>Constructor.</summary>
         * <param name="typeId">Type ID.</param>
         * <param name="mapper">Field mapper.</param>
         */
        public GridClientPortableWriteFrame(int typeId, GridClientPortableIdResolver mapper)
        {
            this.typeId = typeId;
            this.mapper = mapper;
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
         * <summary>Raw mode.</summary>
         */
        public bool Raw
        {
            get { return raw; }
            set { raw = value; }
        }

        /**
         * <summary>Raw position.</summary>
         */
        public long RawPosition
        {
            get { return rawPos; }
            set { rawPos = value; }
        }
    }
}
