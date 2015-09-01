/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Expiry
{
    using System;

    /// <summary>
    /// Default expiry policy implementation with all durations deinfed explicitly.
    /// </summary>
    public class ExpiryPolicy : IExpiryPolicy
    {
        /** Expiry for create. */
        private readonly TimeSpan? create;

        /** Expiry for update. */
        private readonly TimeSpan? update;

        /** Expiry for access. */
        private readonly TimeSpan? access;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="create">Expiry for create.</param>
        /// <param name="update">Expiry for udpate.</param>
        /// <param name="access">Expiry for access.</param>
        public ExpiryPolicy(TimeSpan? create, TimeSpan? update, TimeSpan? access)
        {
            this.create = create;
            this.update = update;
            this.access = access;
        }

        /// <summary>
        /// Gets expiry for create operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired
        /// and will not be added to cache. 
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for create opeartion.</returns>
        public TimeSpan? GetExpiryForCreate()
        {
            return create;
        }

        /// <summary>
        /// Gets expiry for update operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired.
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for update operation.</returns>
        public TimeSpan? GetExpiryForUpdate()
        {
            return update;
        }

        /// <summary>
        /// Gets expiry for access operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired.
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for access operation.</returns>
        public TimeSpan? GetExpiryForAccess()
        {
            return access;
        }
    }
}
