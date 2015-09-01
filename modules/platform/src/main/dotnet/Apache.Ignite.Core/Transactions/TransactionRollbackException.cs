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
    using Apache.Ignite.Core.Common;
    using GridGain.Common;

    /// <summary>
    /// Exception thrown whenever grid transactions has been automatically rolled back.  
    /// </summary>
    [Serializable]
    public class TransactionRollbackException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionRollbackException"/> class.
        /// </summary>
        public TransactionRollbackException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionRollbackException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TransactionRollbackException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionRollbackException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected TransactionRollbackException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionRollbackException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public TransactionRollbackException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}
