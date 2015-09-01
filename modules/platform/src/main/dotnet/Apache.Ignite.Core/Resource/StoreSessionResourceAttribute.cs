/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Resource
{
    using System;

    using GridGain.Cache.Store;

    /// <summary>
    /// Annotates a field or a setter method for injection of current <see cref="ICacheStoreSession"/>
    /// instance. It can be injected into <see cref="ICacheStore"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Method | AttributeTargets.Property)]
    public sealed class StoreSessionResourceAttribute : Attribute
    {
        // No-op.
    }
}
