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
    /// <summary>
    /// Cache transaction state.
    /// </summary>
    public enum TransactionState
    {
        /// <summary>
        /// Transaction started.
        /// </summary>
        ACTIVE,

        /// <summary>
        /// Transaction validating.
        /// </summary>
        PREPARING,

        /// <summary>
        /// Transaction validation succeeded.
        /// </summary>
        PREPARED,

        /// <summary>
        /// Transaction is marked for rollback.
        /// </summary>
        MARKED_ROLLBACK,

        /// <summary>
        /// Transaction commit started (validating finished).
        /// </summary>
        COMMITTING,

        /// <summary>
        /// Transaction commit succeeded.
        /// </summary>
        COMMITTED,
        
        /// <summary>
        /// Transaction rollback started (validation failed).
        /// </summary>
        ROLLING_BACK,

        /// <summary>
        /// Transaction rollback succeeded.
        /// </summary>
        ROLLED_BACK,

        /// <summary>
        /// Transaction rollback failed or is otherwise unknown state.
        /// </summary>
        UNKNOWN
    }
}
