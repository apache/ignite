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
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception thrown from non-transactional cache in case when update succeeded only partially.
    /// </summary>
    [Serializable]
    public class CachePartialUpdateException : CacheException
    {
        /** Serializer key. */
        private const string KEY_FAILED_KEYS = "FailedKeys";

        /** Failed keys. */
        private readonly IList<object> failedKeys;

        /** Failed keys exception. */
        private readonly Exception failedKeysException;

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePartialUpdateException"/> class.
        /// </summary>
        public CachePartialUpdateException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePartialUpdateException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CachePartialUpdateException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CachePartialUpdateException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected CachePartialUpdateException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            failedKeys = (IList<object>) info.GetValue(KEY_FAILED_KEYS, typeof (IList<object>));
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        /// <param name="failedKeysException">Exception occurred during failed keys read/write.</param>
        public CachePartialUpdateException(string msg, Exception failedKeysException) : this(msg, null, failedKeysException)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        /// <param name="failedKeys">Failed keys.</param>
        public CachePartialUpdateException(string msg, IList<object> failedKeys) : this(msg, failedKeys, null)
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="msg">Exception message.</param>
        /// <param name="failedKeys">Failed keys.</param>
        /// <param name="failedKeysException">Exception occurred during failed keys read/write.</param>
        private CachePartialUpdateException(string msg, IList<object> failedKeys, Exception failedKeysException) : base(msg)
        {
            this.failedKeys = failedKeys;
            this.failedKeysException = failedKeysException;
        }

        /// <summary>
        /// Gets the failed keys.
        /// </summary>
        public IEnumerable<T> GetFailedKeys<T>()
        {
            if (failedKeysException != null)
                throw failedKeysException;
            
            return failedKeys == null ? null : failedKeys.Cast<T>();
        }

        /** <inheritdoc /> */
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(KEY_FAILED_KEYS, failedKeys);

            base.GetObjectData(info, context);
        }
    }
}
