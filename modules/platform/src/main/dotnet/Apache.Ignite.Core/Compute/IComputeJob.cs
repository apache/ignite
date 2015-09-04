/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Compute
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Defines executable unit for <see cref="IComputeTask{A,T,R}"/>. Ignite task gets split into jobs
    /// when <see cref="IComputeTask{A,T,R}.Map(IList{IClusterNode}, A)"/> method is called. This
    /// method returns all jobs for the task mapped to their corresponding Ignite nodes for execution. 
    /// Grid will then serialize this jobs and send them to requested nodes for execution.
    /// <para />
    /// Once job execution is complete, the return value will be sent back to parent task and will 
    /// be passed into 
    /// <see cref="IComputeTask{A,T,R}.Result(IComputeJobResult{T}, IList{IComputeJobResult{T}})"/>
    /// method via <see cref="IComputeJobResult{T}"/> instance. 
    /// <para />
    /// Ignite job implementation can be injected with <see cref="IIgnite"/> using 
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
