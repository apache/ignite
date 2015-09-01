/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Compute
{
    using System;

    /// <summary>
    /// This attribute disables caching of task results when attached to <see cref="IComputeTask{A,T,R}"/> 
    /// instance. Use it when number of jobs within task grows too big, or jobs themselves are too large 
    /// to keep in memory throughout task execution. By default all results are cached and passed into
    /// <see cref="IComputeTask{A,T,R}.Result"/> 
    /// and <see cref="IComputeTask{A,T,R}.Reduce"/> methods. When this 
    /// attribute is attached to a task class, then this list of job results will always be empty.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface)]
    public sealed class ComputeTaskNoResultCacheAttribute : Attribute
    {
        // No-op.
    }
}
