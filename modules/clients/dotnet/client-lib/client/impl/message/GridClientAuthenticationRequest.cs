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

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Client authentication request.</summary> */
    [GridClientPortableId(PU.TYPE_AUTH_REQ)]
    internal class GridClientAuthenticationRequest : GridClientRequest {
        /** 
         * <summary>
         * Constructs authentication request.</summary> 
         * 
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientAuthenticationRequest(Guid destNodeId) : base(destNodeId) { 
        }

        /** <summary>Credentials object.</summary> */
        public Object Credentials {
            get;
            set;
        }

        /** <inheritdoc /> */
        public override void WritePortable(IGridClientPortableWriter writer)
        {
            base.WritePortable(writer);

            writer.RawWriter().WriteObject(Credentials);
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridClientPortableReader reader) {
            base.ReadPortable(reader);

            Credentials = reader.RawReader().ReadObject<Object>();
        }
    }
}
