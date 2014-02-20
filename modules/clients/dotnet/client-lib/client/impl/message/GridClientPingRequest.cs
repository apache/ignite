// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using System;

    /** <summary>Fictive ping packet.</summary> */
    internal class GridClientPingRequest : GridClientRequest {
        /**
         * <summary>
         * Constructs fictive ping packet.</summary>
         * 
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientPingRequest(Guid destNodeId) : base(destNodeId) { 
        }
    }
}
