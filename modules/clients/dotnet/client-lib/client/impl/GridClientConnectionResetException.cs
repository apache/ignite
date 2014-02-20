// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Runtime.Serialization;

    /**
     * <summary>
     * This exception is thrown when ongoing packet should be sent, but network connection is broken.
     * In this case client will try to reconnect to any of the servers specified in configuration.</summary>
     */
    [Serializable]
    internal class GridClientConnectionResetException : GridClientException {
        /** <summary>Constructs an exception.</summary> */
        public GridClientConnectionResetException() {
        }

        /**
         * <summary>
         * Creates an exception with given message.</summary>
         *
         * <param name="msg">Error message.</param>
         */
        public GridClientConnectionResetException(String msg)
            : base(msg) {
        }

        /**
         * <summary>
         * Creates an exception with given message and error cause.</summary>
         *
         * <param name="msg">Error message.</param>
         * <param name="cause">Wrapped exception.</param>
         */
        public GridClientConnectionResetException(String msg, Exception cause)
            : base(msg, cause) {
        }

        /**
         * <summary>
         * Constructs an exception.</summary>
         *
         * <param name="info">Serialization info.</param>
         * <param name="ctx">Streaming context.</param>
         */
        protected GridClientConnectionResetException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
