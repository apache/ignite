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
     * This exception is thrown if client was closed by idle checker thread. This exception should be
     * handled internally and never rethrown to user.</summary>
     */
    [Serializable]
    internal class GridClientConnectionIdleClosedException : GridClientConnectionResetException {
        /** <summary>Constructs an exception.</summary> */
        public GridClientConnectionIdleClosedException() {
        }

        /**
         * <summary>
         * Creates exception with error message.</summary>
         *
         * <param name="msg">Error message.</param>
         */
        public GridClientConnectionIdleClosedException(String msg)
            : base(msg) {
        }


        /**
         * <summary>
         * Creates an exception with given message and error cause.</summary>
         *
         * <param name="msg">Exception message.</param>
         * <param name="cause">Exception cause.</param>
         */
        public GridClientConnectionIdleClosedException(String msg, Exception cause)
            : base(msg, cause) {
        }

        /**
         * <summary>
         * Constructs an exception.</summary>
         *
         * <param name="info">Serialization info.</param>
         * <param name="ctx">Streaming context.</param>
         */
        protected GridClientConnectionIdleClosedException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
