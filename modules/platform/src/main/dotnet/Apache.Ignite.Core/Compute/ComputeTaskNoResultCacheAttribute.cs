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
