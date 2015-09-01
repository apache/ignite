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

    using GridGain.Compute;

    /// <summary>
    /// Attribute which injects <see cref="GridGain.IGrid"/> instance. Can be defined inside
    /// implementors of <see cref="IComputeTask{A,T,R}"/> and <see cref="IComputeJob"/> interfaces.
    /// Can be applied to non-static fields, properties and methods returning <c>void</c> and 
    /// accepting a single parameter.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Method | AttributeTargets.Property)]
    public sealed class InstanceResourceAttribute : Attribute
    {
        // No-op.
    }
}
