/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cluster 
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;
    using GridGain.Common;

    /// <summary>
    /// Indicates an illegal call on empty projection. Thrown by projection when operation
    /// that requires at least one node is called on empty projection.
    /// </summary>
    [Serializable]
    public class ClusterGroupEmptyException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterGroupEmptyException"/> class.
        /// </summary>
        public ClusterGroupEmptyException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterGroupEmptyException"/> class.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        public ClusterGroupEmptyException(string msg) : base(msg)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterGroupEmptyException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ClusterGroupEmptyException(string message, Exception cause)
            : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterGroupEmptyException"/> class.
        /// </summary>
        /// <param name="info">Serialization info.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ClusterGroupEmptyException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }
    }
}
