/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Query
{
    using System;
    using System.Collections;
    using System.Collections.ObjectModel;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /**
     * 
     */
    [GridClientPortableId(PU.TYPE_CACHE_QUERY_RESULT)]
    class GridClientDataQueryResult : IGridClientPortable {
        /**
         * 
         */
        public long QueryId {
            get;
            set;
        }

        /**
         * 
         */
        public ICollection Items {
            get;
            set;
        }

        /**
         * 
         */
        public bool Last {
            get;
            set;
        }

        /**
         * 
         */
        public Guid NodeId {
            get;
            set;
        }

        /** <inheritdoc /> */
        public void WritePortable(IGridClientPortableWriter writer) {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteLong(QueryId);
            rawWriter.WriteCollection(Items);
            rawWriter.WriteBoolean(Last);
            rawWriter.WriteGuid(NodeId);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader) {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            QueryId = rawReader.ReadLong();
            Items = (ICollection)rawReader.ReadCollection();
            Last = rawReader.ReadBoolean();
            NodeId = rawReader.ReadGuid().Value;
        }
    }
}