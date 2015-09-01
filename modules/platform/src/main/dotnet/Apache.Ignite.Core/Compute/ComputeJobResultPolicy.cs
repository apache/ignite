/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Compute
{
    using System.Collections.Generic;

    /// <summary>
    /// This enumeration provides different types of actions following the last received job result. See 
    /// <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
    /// for more details.
    /// </summary>
    public enum ComputeJobResultPolicy
    {
        /// <summary>
        /// Wait for results if any are still expected. If all results have been received -
        /// it will start reducing results.
        /// </summary>
        WAIT = 0,

        /// <summary>
        /// Ignore all not yet received results and start reducing results.
        /// </summary>
        REDUCE = 1,

        /// <summary>
        /// Fail-over job to execute on another node.
        /// </summary>
        FAILOVER = 2
    }
}
