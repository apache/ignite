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
    using System.Collections.Generic;

    using GridGain.Cluster;
    using GridGain.Resource;

    /// <summary>
    /// Defines executable unit for <see cref="IComputeTask{A,T,R}"/>. Grid task gets split into jobs
    /// when <see cref="IComputeTask{A,T,R}.Map(IList{IClusterNode}, A)"/> method is called. This
    /// method returns all jobs for the task mapped to their corresponding grid nodes for execution. 
    /// Grid will then serialize this jobs and send them to requested nodes for execution.
    /// <para />
    /// Once job execution is complete, the return value will be sent back to parent task and will 
    /// be passed into 
    /// <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
    /// method via <see cref="IComputeJobResult{T}"/> instance. 
    /// <para />
    /// Grid job implementation can be injected with <see cref="IIgnite"/> using 
    /// <see cref="InstanceResourceAttribute"/> attribute.
    /// </summary>
    public interface IComputeJob<out T>
    {
        /// <summary>
        /// Executes this job.
        /// </summary>
        /// <returns>Job execution result (possibly <c>null</c>). This result will be returned
        /// in <see cref="IComputeJobResult{T}"/> object passed into 
        /// <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
        /// on caller node.</returns>
        T Execute();

        /// <summary>
        /// This method is called when system detects that completion of this
        /// job can no longer alter the overall outcome (for example, when parent task
        /// has already reduced the results). 
        /// <para />
        /// Note that job cancellation is only a hint, and it is really up to the actual job
        /// instance to gracefully finish execution and exit.
        /// </summary>
        void Cancel();
    }
}
