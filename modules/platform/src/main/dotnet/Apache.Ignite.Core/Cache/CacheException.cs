/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;
    using GridGain.Common;

    /// <summary>
    /// Indicates an error during Cache operation.
    /// </summary>
    [Serializable]
    public class CacheException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheException"/> class.
        /// </summary>
        public CacheException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CacheException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public CacheException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected CacheException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }
    }
}