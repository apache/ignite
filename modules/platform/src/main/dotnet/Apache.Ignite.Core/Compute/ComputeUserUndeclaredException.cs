/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Compute 
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;
    using GridGain.Common;

    /// <summary>
    /// This exception is thrown when user's code throws undeclared runtime exception. By user core it is
    /// assumed the code in grid task, grid job or SPI. In most cases it should be an indication of unrecoverable
    /// error condition such as assertion, out of memory error, etc.
    /// </summary>
    [Serializable]
    public class ComputeUserUndeclaredException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeUserUndeclaredException"/> class.
        /// </summary>
        public ComputeUserUndeclaredException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeUserUndeclaredException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ComputeUserUndeclaredException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeUserUndeclaredException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ComputeUserUndeclaredException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeUserUndeclaredException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ComputeUserUndeclaredException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}
