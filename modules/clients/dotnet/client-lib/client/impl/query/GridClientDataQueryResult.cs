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

    /**
     * 
     */
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
            writer.WriteLong("queryId", QueryId);

            writer.WriteCollection("items", Items);
            writer.WriteBoolean("last", Last);
            writer.WriteGuid("nodeId", NodeId);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader) {
            QueryId = reader.ReadLong("queryId");

            Items = (ICollection)reader.ReadCollection("items");
            Last = reader.ReadBoolean("last");
            NodeId = reader.ReadGuid("nodeId").Value;
        }
    }
}