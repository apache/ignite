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
    /// Indicates an error with grid topology (e.g., crashed node, etc.)
    /// </summary>
    [Serializable]
    public class ClusterTopologyException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTopologyException"/> class.
        /// </summary>
        public ClusterTopologyException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTopologyException"/> class.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        public ClusterTopologyException(string msg) : base(msg)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTopologyException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public ClusterTopologyException(string message, Exception cause)
            : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterTopologyException"/> class.
        /// </summary>
        /// <param name="info">Serialization info.</param>
        /// <param name="ctx">Streaming context.</param>
        protected ClusterTopologyException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }
    }
}