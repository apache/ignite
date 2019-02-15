/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Compute
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Defines executable unit for <see cref="IComputeTask{A,T,R}"/>. Ignite task gets split into jobs
    /// when <see cref="IComputeTask{A,T,R}.Map(IList{IClusterNode}, A)"/> method is called. This
    /// method returns all jobs for the task mapped to their corresponding Ignite nodes for execution. 
    /// Grid will then serialize this jobs and send them to requested nodes for execution.
    /// <para />
    /// Once job execution is complete, the return value will be sent back to parent task and will 
    /// be passed into 
    /// <see cref="IComputeTask{TA,T,TR}.OnResult"/>
    /// method via <see cref="IComputeJobResult{T}"/> instance. 
    /// <para />
    /// Ignite job implementation can be injected with <see cref="IIgnite"/> using 
    /// <see cref="InstanceResourceAttribute"/> attribute.
    /// </summary>
    public interface IComputeJob<out TRes>
    {
        /// <summary>
        /// Executes this job.
        /// </summary>
        /// <returns>Job execution result (possibly <c>null</c>). This result will be returned
        /// in <see cref="IComputeJobResult{T}"/> object passed into 
        /// <see cref="IComputeTask{TA,T,TR}.OnResult"/>
        /// on caller node.</returns>
        TRes Execute();

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
