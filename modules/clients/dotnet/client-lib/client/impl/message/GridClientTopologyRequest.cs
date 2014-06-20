/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using System.Text;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary><c>Topology</c> command request.</summary> */
    [GridClientPortableId(PU.TYPE_TOP_REQ)]
    internal class GridClientTopologyRequest : GridClientRequest {
        /**
         * <summary>
         * Constructs topology request.</summary>
         *
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientTopologyRequest(Guid destNodeId) : base(destNodeId) {
        }

        /** <summary>Include metrics flag.</summary> */
        public bool IncludeMetrics {
            get;
            set;
        }

        /** <summary>Include node attributes flag.</summary> */
        public bool IncludeAttributes {
            get;
            set;
        }

        /** <summary>Id of requested node if specified, <c>null</c> otherwise.</summary> */
        public Guid NodeId {
            get;
            set;
        }

        /** <summary>IP address of requested node if specified, <c>null</c> otherwise.</summary> */
        public String NodeIP {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override void WritePortable(IGridClientPortableWriter writer) {
            base.WritePortable(writer);

            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteGuid(NodeId);
            rawWriter.WriteString(NodeIP);

            rawWriter.WriteBoolean(IncludeMetrics);
            rawWriter.WriteBoolean(IncludeAttributes);
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridClientPortableReader reader) {
            base.ReadPortable(reader);

            IGridClientPortableRawReader rawReader = reader.RawReader();

            NodeId = rawReader.ReadGuid();
            NodeIP = rawReader.ReadString();

            IncludeMetrics = rawReader.ReadBoolean();
            IncludeAttributes = rawReader.ReadBoolean();
        }
    }
}
