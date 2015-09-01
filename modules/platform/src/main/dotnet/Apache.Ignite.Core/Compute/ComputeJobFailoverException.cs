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
    /// This runtime exception can be thrown from <see cref="IComputeJob{T}.Execute()"/>
    /// method to force job failover to another node within task topology.
    /// <see cref="IComputeFunc{T,R}"/> or <see cref="IComputeFunc{T}"/>
    /// passed into any of the <see cref="ICompute"/> methods can also throw this exception
    /// to force failover.
    /// </summary>
    [Serializable]
    public class ComputeJobFailoverException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException"/> class.
        /// </summary>
        public ComputeJobFailoverException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException" /> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ComputeJobFailoverException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ComputeJobFailoverException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobFailoverException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ComputeJobFailoverException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}
