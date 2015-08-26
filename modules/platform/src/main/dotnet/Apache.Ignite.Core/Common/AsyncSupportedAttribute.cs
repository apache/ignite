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

namespace Apache.Ignite.Core.Common
{
    using System;

    /// <summary>
    /// Attribute to indicate that method can be executed asynchronously if async mode is enabled.
    /// To enable async mode, invoke <see cref="IAsyncSupport{TWithAsync}.WithAsync"/> method on the API.
    /// The future for the async method can be retrieved via 
    /// <see cref="IFuture{T}"/> right after the execution of an asynchronous method.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class AsyncSupportedAttribute : Attribute
    {
        // No-op.
    }
}