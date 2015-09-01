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
    /// Defines functions to determine when cache entries will expire based on
    /// creation, access and modification operations.
    /// </summary>
    public interface IExpiryPolicy
    {
        /// <summary>
        /// Gets expiry for create operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired
        /// and will not be added to cache. 
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for create opeartion.</returns>
        TimeSpan? GetExpiryForCreate();

        /// <summary>
        /// Gets expiry for update operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired.
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for update operation.</returns>
        TimeSpan? GetExpiryForUpdate();

        /// <summary>
        /// Gets expiry for access operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired.
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for access operation.</returns>
        TimeSpan? GetExpiryForAccess();
    }
}
