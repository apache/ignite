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

    /** <summary>Request for a log file.</summary> */
    internal class GridClientLogRequest : GridClientRequest {
        /** 
         * <summary>
         * Constructs log request.</summary>
         * 
         * <param name="destNodeId">Node ID to route request to.</param>
         */
        public GridClientLogRequest(Guid destNodeId)
            : base(destNodeId) {
            From = -1;
            To = -1;
        }

        /** <summary>Path to log file.</summary> */
        public String Path {
            get;
            set;
        }

        /** <summary>From line, inclusive, indexing from 0.</summary> */
        public int From {
            get;
            set;
        }

        /** <summary>To line, inclusive, indexing from 0, can exceed count of lines in log.</summary> */
        public int To {
            get;
            set;
        }
    }
}
