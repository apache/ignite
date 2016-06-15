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
