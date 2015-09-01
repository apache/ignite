/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Transactions
{
    /// <summary>
    /// Defines different cache transaction isolation levels. See <see cref="ITransaction"/>
    /// documentation for more information about cache transaction isolation levels.
    /// </summary>
    public enum TransactionIsolation
    {
        /// <summary>
        /// Read committed isolation level.
        /// </summary>
        READ_COMMITTED = 0,

        /// <summary>
        /// Repeatable read isolation level.
        /// </summary>
        REPEATABLE_READ = 1,

        /// <summary>
        /// Serializable isolation level.
        /// </summary>
        SERIALIZABLE = 2
    }
}
