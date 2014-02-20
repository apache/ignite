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

    /** <summary>This exception is thrown whenever a closed client is attempted to be used.</summary> */
    [Serializable]
    public class GridClientClosedException : GridClientException {
        /** <summary>Constructs an exception.</summary> */
        public GridClientClosedException() {
        }

        /**
         * <summary>
         * Creates exception with given message.</summary>
         *
         * <param name="msg">Error message.</param>
         */
        public GridClientClosedException(String msg)
            : base(msg) {
        }

        /**
         * <summary>
         * Creates exception with given message and cause.</summary>
         *
         * <param name="msg">Message.</param>
         * <param name="cause">Cause.</param>
         */
        public GridClientClosedException(String msg, Exception cause)
            : base(msg, cause) {
        }

        /**
         * <summary>
         * Constructs an exception.</summary>
         *
         * <param name="info">Serialization info.</param>
         * <param name="ctx">Streaming context.</param>
         */
        protected GridClientClosedException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
