/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Portable 
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;
    using GridGain.Common;

    /// <summary>
    /// Indicates an error during portable marshalling.
    /// </summary>
    [Serializable]
    public class PortableException : IgniteException
    {
        /// <summary>
        /// Constructs an exception. 
        /// </summary>
        public PortableException() 
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public PortableException(string message)
            : base(message) {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public PortableException(string message, Exception cause)
            : base(message, cause) {
        }

        /// <summary>
        /// Constructs an exception.
        /// </summary>
        /// <param name="info">Serialization info.</param>
        /// <param name="ctx">Streaming context.</param>
        protected PortableException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx) {
        }
    }
}
