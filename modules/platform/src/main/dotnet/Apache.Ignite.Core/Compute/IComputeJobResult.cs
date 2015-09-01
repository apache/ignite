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
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Job execution result which gets passed to 
    /// <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
    /// method.
    /// </summary>
    public interface IComputeJobResult<out T>
    {
        /// <summary>
        /// Gets data returned by remote job if it didn't fail. This data is the
        /// object returned from <see cref="IComputeJob{T}.Execute()"/> method.
        /// <para />
        /// Note that if task is annotated with <see cref="ComputeTaskNoResultCacheAttribute"/> 
        /// attribute, then job results will not be cached and will be available only in
        /// <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
        /// method for every individual job, but not in 
        /// <see cref="IComputeTask{A,T,R}.Reduce(IList{IComputeJobResult{T}})"/> method.
        /// 
        /// </summary>
        /// <returns>Data returned by job.</returns>
        T Data();

        /// <summary>
        /// Gets local instance of remote job produced this result.
        /// </summary>
        /// <returns></returns>
        IComputeJob<T> Job();

        /// <summary>
        /// Gets exception produced by execution of remote job, or <c>null</c> if no
        /// exception was produced.
        /// </summary>
        /// <returns>Exception or <c>null</c> in case of success.</returns>
        Exception Exception();

        /// <summary>
        /// ID of the node where actual job execution occurred.
        /// </summary>
        Guid NodeId
        {
            get;
        }

        /// <summary>
        /// Whether the job was cancelled.
        /// </summary>
        bool Cancelled
        {
            get;
        }
    }
}
