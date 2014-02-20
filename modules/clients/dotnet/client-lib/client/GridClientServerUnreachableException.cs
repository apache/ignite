// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Runtime.Serialization;

    /**
     * <summary>
     * This exception is thrown when none of the servers specified in client configuration can be
     * connected to within timeout.</summary>
     */
    [Serializable]
    public class GridClientServerUnreachableException : GridClientException {
        /** <summary>Constructs an exception.</summary> */
        public GridClientServerUnreachableException() {
        }

        /**
         * <summary>
         * Creates exception with specified error message.</summary>
         *
         * <param name="msg">Error message.</param>
         */
        public GridClientServerUnreachableException(String msg)
            : base(msg) {
        }

        /**
         * <summary>
         * Creates exception with specified error message and cause.</summary>
         *
         * <param name="msg">Error message.</param>
         * <param name="cause">Error cause.</param>
         */
        public GridClientServerUnreachableException(String msg, Exception cause)
            : base(msg, cause) {
        }

        /**
         * <summary>
         * Constructs an exception.</summary>
         *
         * <param name="info">Serialization info.</param>
         * <param name="ctx">Streaming context.</param>
         */
        protected GridClientServerUnreachableException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
