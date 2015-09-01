/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache
{
    using System;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// An exception to indicate a problem occurred attempting to execute an 
    /// <see cref="ICacheEntryProcessor{K, V, A, R}"/> against an entry.
    /// </summary>
    [Serializable]
    public class CacheEntryProcessorException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        public CacheEntryProcessorException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CacheEntryProcessorException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public CacheEntryProcessorException(string message, Exception cause)
            : base(message, cause)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="innerException">The inner exception.</param>
        public CacheEntryProcessorException(Exception innerException)
            : base("Error occurred in CacheEntryProcessor, see InnerException for details.", innerException)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheEntryProcessorException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected CacheEntryProcessorException(SerializationInfo info, StreamingContext ctx) : base(info, ctx)
        {
            // No-op.
        }
    }
}