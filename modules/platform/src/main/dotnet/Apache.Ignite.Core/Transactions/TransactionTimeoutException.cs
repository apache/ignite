/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Transactions 
{
    using System;
    using System.Runtime.Serialization;

    using GridGain.Common;

    /// <summary>
    /// Exception thrown whenever grid transactions time out.  
    /// </summary>
    [Serializable]
    public class TransactionTimeoutException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionTimeoutException"/> class.
        /// </summary>
        public TransactionTimeoutException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionTimeoutException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TransactionTimeoutException(string message)
            : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionTimeoutException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected TransactionTimeoutException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionTimeoutException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public TransactionTimeoutException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}
