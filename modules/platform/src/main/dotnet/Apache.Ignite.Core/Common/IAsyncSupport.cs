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
    /// <summary>
    /// Allows to enable asynchronous mode on Grid APIs.
    /// </summary>
    /// <typeparam name="TWithAsync">Type of WithAsync method result.</typeparam>
    public interface IAsyncSupport<out TWithAsync> where TWithAsync : IAsyncSupport<TWithAsync>
    {
        /// <summary>
        /// Gets component with asynchronous mode enabled.
        /// </summary>
        /// <returns>Component with asynchronous mode enabled.</returns>
        TWithAsync WithAsync();

        /// <summary>
        /// Gets a value indicating whether this instance is in asynchronous mode.
        /// </summary>
        /// <value>
        /// <c>true</c> if asynchronous mode is enabled.
        /// </value>
        bool IsAsync { get; }

        /// <summary>
        /// Gets and resets future for previous asynchronous operation.
        /// </summary>
        /// <returns>Future for previous asynchronous operation.</returns>
        IFuture GetFuture();

        /// <summary>
        /// Gets and resets future for previous asynchronous operation.
        /// </summary>
        /// <returns>Future for previous asynchronous operation.</returns>
        IFuture<TResult> GetFuture<TResult>();
    }
}