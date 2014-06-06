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

    /** <summary><c>Topology</c> command request.</summary> */
    internal class GridClientTopologyRequest : GridClientRequest {
        public const int PORTABLE_TYPE_ID = -9;
        
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

        public override int TypeId {
            get { return PORTABLE_TYPE_ID; }
        }

        public override void WritePortable(IGridPortableWriter writer) {
            base.WritePortable(writer);

            writer.WriteGuid("nodeId", NodeId);

            writer.WriteString("nodeIp", NodeIP);

            writer.WriteBoolean("includeMetrics", IncludeMetrics);
            writer.WriteBoolean("includeAttrs", IncludeAttributes);
        }

        public override void ReadPortable(IGridPortableReader reader) {
            base.ReadPortable(reader);

            NodeId = reader.ReadGuid("nodeId");

            NodeIP = reader.ReadString("nodeIp");

            IncludeMetrics = reader.ReadBoolean("includeMetrics");
            IncludeAttributes = reader.ReadBoolean("includeAttrs");
        }
    }
}
