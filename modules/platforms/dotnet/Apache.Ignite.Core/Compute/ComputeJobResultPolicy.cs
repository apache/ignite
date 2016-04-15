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
    /// <summary>
    /// This enumeration provides different types of actions following the last received job result. See 
    /// <see cref="IComputeTask{TA,T,TR}.OnResult"/>
    /// for more details.
    /// </summary>
    public enum ComputeJobResultPolicy
    {
        /// <summary>
        /// Wait for results if any are still expected. If all results have been received -
        /// it will start reducing results.
        /// </summary>
        Wait = 0,

        /// <summary>
        /// Ignore all not yet received results and start reducing results.
        /// </summary>
        Reduce = 1,

        /// <summary>
        /// Fail-over job to execute on another node.
        /// </summary>
        Failover = 2
    }
}
