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
    /// Exception thrown whenever grid transaction enters an unknown state.
    /// This exception is usually thrown whenever commit partially succeeds.
    /// Cache will still resolve this situation automatically to ensure data
    /// integrity, by invalidating all values participating in this transaction
    /// on remote nodes.  
    /// </summary>
    [Serializable]
    public class TransactionHeuristicException : IgniteException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionHeuristicException"/> class.
        /// </summary>
        public TransactionHeuristicException()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionHeuristicException"/> class.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public TransactionHeuristicException(string message) : base(message)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionHeuristicException"/> class.
        /// </summary>
        /// <param name="info">Serialization information.</param>
        /// <param name="ctx">Streaming context.</param>
        protected TransactionHeuristicException(SerializationInfo info, StreamingContext ctx)
            : base(info, ctx)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionHeuristicException"/> class.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="cause">The cause.</param>
        public TransactionHeuristicException(string message, Exception cause) : base(message, cause)
        {
            // No-op.
        }
    }
}
