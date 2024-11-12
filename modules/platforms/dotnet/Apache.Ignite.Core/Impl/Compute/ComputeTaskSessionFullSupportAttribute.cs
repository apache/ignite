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
    using static System.AttributeTargets;

    /// <summary>
    /// Enables distributing <see cref="IComputeTaskSession"/>'s attributes from
    /// <see cref="IComputeTask{TArg,TJobRes,TRes}"/> to <see cref="IComputeJob{TRes}"/> that the task creates.
    /// <see cref="IComputeTask{TArg,TJobRes,TRes}"/> implementations must be annotated with the
    /// <see cref="ComputeTaskSessionFullSupportAttribute"/> to enable the features depending on the
    /// <see cref="IComputeTaskSession"/> attributes.
    /// By default attributes and checkpoints are disabled for performance reasons.
    /// </summary>
    [AttributeUsage(Class | Struct)]
    public sealed class ComputeTaskSessionFullSupportAttribute : Attribute
    {
        // No-op.
    }
}