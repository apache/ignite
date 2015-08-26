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
    using System.Threading.Tasks;

    /// <summary>
    /// Non-generic Future. Represents an asynchronous operation that can return a value.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IFuture
    {
        /// <summary>
        /// Gets a value indicating whether this instance is done.
        /// </summary>
        bool IsDone
        {
            get;
        }

        /// <summary>
        /// Gets the future result.
        /// </summary>
        /// <returns>Future result.</returns>
        object Get();

        /// <summary>
        /// Gets the future result with a timeout.
        /// </summary>
        /// <param name="timeout">The timeout.</param>
        /// <returns>
        /// Future result, if it is obtained within specified timeout; otherwise, throws <see cref="TimeoutException"/>
        /// </returns>
        /// <exception cref="TimeoutException">Thrown if Get operation exceeded specified timeout.</exception>
        object Get(TimeSpan timeout);

        /// <summary>
        /// Listens this instance and invokes callback upon future completion.
        /// </summary>
        /// <param name="callback">The callback to execute upon future completion.</param>
        void Listen(Action callback);

        /// <summary>
        /// Listens this instance and invokes callback upon future completion.
        /// </summary>
        /// <param name="callback">The callback to execute upon future completion.</param>
        void Listen(Action<IFuture> callback);

        /// <summary>
        /// Gets an IAsyncResult indicating the state of this Future.
        /// </summary>
        /// <returns>Future state representation in form of IAsyncResult.</returns>
        IAsyncResult ToAsyncResult();

        /// <summary>
        /// Gets a Task that returns the result of this Future.
        /// </summary>
        /// <returns>Task that completes when this future gets done and returns the result.</returns>
        Task<object> ToTask();
    }

    /// <summary>
    /// Generic Future. Represents an asynchronous operation that can return a value.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    /// <typeparam name="T">Future result type.</typeparam>
    public interface IFuture<T> : IFuture
    {
        /// <summary>
        /// Gets the future result.
        /// </summary>
        /// <returns>Future result.</returns>
        new T Get();

        /// <summary>
        /// Gets the future result with a timeout.
        /// </summary>
        /// <param name="timeout">The timeout.</param>
        /// <returns>
        /// Future result, if it is obtained within specified timeout; otherwise, throws <see cref="TimeoutException"/>
        /// </returns>
        /// <exception cref="TimeoutException">Thrown if Get operation exceeded specified timeout.</exception>
        new T Get(TimeSpan timeout);

        /// <summary>
        /// Gets a Task that returns the result of this Future.
        /// </summary>
        /// <returns>Task that completes when this future gets done and returns the result.</returns>
        new Task<T> ToTask();

        /// <summary>
        /// Listens this instance and invokes callback upon future completion.
        /// </summary>
        /// <param name="callback">The callback to execute upon future completion.</param>
        void Listen(Action<IFuture<T>> callback);
    }
}
