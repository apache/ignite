/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;
    using GridGain.Client.Portable;

    /**
     * <summary>
     * This class provides implementation for commit message fields
     * and cannot be used directly.</summary>
     */
    internal abstract class GridClientRequest : IGridClientPortable {
        /** Portable type ID. */
        // TODO: GG-8535: Remove in favor of normal IDs.
        public static readonly int PORTABLE_TYPE_ID = 0;

        /** <summary>Deny no-arg constructor for client requests.</summary> */
        private GridClientRequest() { 
        }

        /**
         * <summary>
         * Creates grid common request.</summary>
         *
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientRequest(Guid destNodeId) {
            this.DestNodeId = destNodeId;
        }

        /** <summary>Request id.</summary> */
        public long RequestId {
            get;
            set;
        }

        /** <summary>Client id.</summary> */
        public Guid ClientId {
            get;
            set;
        }

        /** <summary>Destination node id.</summary> */
        public Guid DestNodeId {
            get;
            private set;
        }

        /** <summary>Client session token.</summary> */
        public byte[] SessionToken {
            get;
            set;
        }
        
        // TODO: GG-8535: Correct type IDs in child classes (should be abstract instead of virtual).
        public virtual int TypeId { get {return 0;} }

        public virtual void WritePortable(IGridClientPortableWriter writer) {
            writer.WriteByteArray("sesTok", SessionToken);
        }

        public virtual void ReadPortable(IGridClientPortableReader reader) {
            SessionToken = reader.ReadByteArray("sesTok");
        }
    }
}
