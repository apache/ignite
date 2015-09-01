/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Compute 
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Indicates a situation when execution service provided by the user in configuration rejects execution.
    /// </summary>
    [Serializable]
    public class ComputeExecutionRejectedException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeExecutionRejectedException"/> class.
        /// </summary>
        public ComputeExecutionRejectedException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeExecutionRejectedException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ComputeExecutionRejectedException(string message)
            : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeExecutionRejectedException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ComputeExecutionRejectedException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeExecutionRejectedException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ComputeExecutionRejectedException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}
