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
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Job execution result which gets passed to 
    /// <see cref="IComputeTask{TA,T,TR}.OnResult"/>
    /// method.
    /// </summary>
    public interface IComputeJobResult<out TRes>
    {
        /// <summary>
        /// Gets data returned by remote job if it didn't fail. This data is the
        /// object returned from <see cref="IComputeJob{T}.Execute()"/> method.
        /// <para />
        /// Note that if task is annotated with <see cref="ComputeTaskNoResultCacheAttribute"/> 
        /// attribute, then job results will not be cached and will be available only in
        /// <see cref="IComputeTask{TA,T,TR}.OnResult"/>
        /// method for every individual job, but not in 
        /// <see cref="IComputeTask{A,T,R}.Reduce(IList{IComputeJobResult{T}})"/> method. 
        /// </summary>
        /// <returns>Data returned by job.</returns>
        TRes Data { get; }

        /// <summary>
        /// Gets local instance of remote job produced this result.
        /// </summary>
        /// <returns></returns>
        IComputeJob<TRes> Job { get; }

        /// <summary>
        /// Gets exception produced by execution of remote job, or <c>null</c> if no
        /// exception was produced.
        /// </summary>
        /// <value>Exception or <c>null</c> in case of success.</value>
        Exception Exception { get; }

        /// <summary>
        /// ID of the node where actual job execution occurred.
        /// </summary>
        Guid NodeId { get; }

        /// <summary>
        /// Whether the job was cancelled.
        /// </summary>
        bool Cancelled { get; }
    }
}
