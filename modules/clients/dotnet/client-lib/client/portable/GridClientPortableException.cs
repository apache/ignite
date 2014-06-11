/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable 
{
    using System;
    using System.Runtime.Serialization;

    /** <summary>Client portable object exception.</summary> */
    [Serializable]
    class GridClientPortableException : Exception
    {
        /** <summary>Constructs an exception.</summary> */
        public GridClientPortableException() {
        }

        /**
         * <summary>Creates an exception with given message.</summary>
         *
         * <param name="msg">Exception message.</param>
         */
        public GridClientPortableException(String msg)
            : base(msg) {
        }

        /**
         * <summary>Creates an exception with given message and error cause.</summary>
         *
         * <param name="msg">Exception message.</param>
         * <param name="cause">Exception cause.</param>
         */
        public GridClientPortableException(String msg, Exception cause)
            : base(msg, cause) {
        }

        /**
         * <summary>Constructs an exception.</summary>
         *
         * <param name="info">Serialization info.</param>
         * <param name="ctx">Streaming context.</param>
         */
        protected GridClientPortableException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
