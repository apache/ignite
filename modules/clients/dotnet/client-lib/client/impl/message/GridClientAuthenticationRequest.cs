/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    /** <summary>Client authentication request.</summary> */
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
        public override void WritePortable(IGridPortableWriter writer)
        {
            base.WritePortable(writer);

            writer.WriteObject("cred", Credentials);
        }

        /** <inheritdoc /> */
        public override void ReadPortable(IGridPortableReader reader) {
            base.ReadPortable(reader);

            Credentials = reader.ReadObject<Object>("cred");
        }
    }
}
