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

    /** <summary>Exception that represents client authentication failure for some reason.</summary> */
    [Serializable]
    public class GridClientAuthenticationException : GridClientException {
        /** <summary>Constructs an exception.</summary> */
        public GridClientAuthenticationException() {
        }

        /**
         * <summary>
         * Creates exception with given message.</summary>
         *
         * <param name="msg">Error message.</param>
         */
        public GridClientAuthenticationException(String msg)
            : base(msg) {
        }

        /**
         * <summary>
         * Creates exception with given message and cause.</summary>
         *
         * <param name="msg">Message.</param>
         * <param name="cause">Cause.</param>
         */
        public GridClientAuthenticationException(String msg, Exception cause)
            : base(msg, cause) {
        }

        /**
         * <summary>
         * Constructs an exception.</summary>
         *
         * <param name="info">Serialization info.</param>
         * <param name="ctx">Streaming context.</param>
         */
        protected GridClientAuthenticationException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
