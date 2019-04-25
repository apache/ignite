/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Resource
{
    using System;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Attribute which injects <see cref="IIgnite"/> instance. Can be defined inside
    /// implementors of <see cref="IComputeTask{A,T,TR}"/> and <see cref="IComputeJob{T}"/> interfaces.
    /// Can be applied to non-static fields, properties and methods returning <c>void</c> and 
    /// accepting a single parameter.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Method | AttributeTargets.Property)]
    public sealed class InstanceResourceAttribute : Attribute
    {
        // No-op.
    }
}
