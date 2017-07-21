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
    /// Convenience adapter for <see cref="IComputeJob{T}"/> implementations. It provides the following functionality:
    /// <ul>
    /// <li>
    ///      Default implementation of <see cref="IComputeJob{T}.Cancel()"/> method and ability
    ///      to check whether cancellation occurred with <see cref="ComputeJobAdapter{T}.IsCancelled()"/> method.
    /// </li>
    /// <li>
    ///      Ability to set and get job arguments via <see cref="ComputeJobAdapter{T}.SetArguments(object[])"/>
    ///      and <see cref="GetArgument{TArg}"/> methods.
    /// </li>
    /// </ul>
    /// </summary>
    [Serializable]
    public abstract class ComputeJobAdapter<T> : IComputeJob<T>
    {
        /** Cancelled flag */
        [NonSerialized]
        private volatile bool _cancelled;

        /** Arguments. */
        private object[] _args;

        /// <summary>
        /// No-arg constructor.
        /// </summary>
        protected ComputeJobAdapter()
        {
            // No-op.
        }

        /// <summary>
        /// Creates job with specified arguments.
        /// </summary>
        /// <param name="args">Optional job arguments.</param>
        protected ComputeJobAdapter(params object[] args)
        {
            _args = args;
        }

        /// <summary>
        /// This method is called when system detects that completion of this
        /// job can no longer alter the overall outcome (for example, when parent task
        /// has already reduced the results).
        /// <para />
        /// Note that job cancellation is only a hint, and it is really up to the actual job
        /// instance to gracefully finish execution and exit.
        /// </summary>
        public void Cancel()
        {
            _cancelled = true;
        }

        /// <summary>
        /// Sets given arguments.
        /// </summary>
        /// <param name="args">Optional job arguments to set.</param>
        public void SetArguments(params object[] args)
        {
            _args = args;
        }

        /// <summary>
        /// Sets given arguments.
        /// </summary>
        /// <param name="idx">Index of the argument.</param>
        public TArg GetArgument<TArg>(int idx)
        {
            if (_args == null || idx < 0 || idx >= _args.Length)
                throw new ArgumentOutOfRangeException("Invalid argument index: " + idx);

            return (TArg)_args[idx];
        }

        /// <summary>
        /// This method tests whether or not this job was cancelled. This method
        /// is thread-safe and can be called without extra synchronization.
        /// <p/>
        /// This method can be periodically called in <see cref="IComputeJob{T}.Execute()"/> method
        /// implementation to check whether or not this job cancelled. Note that system
        /// calls <see cref="IComputeJob{T}.Cancel()"/> method only as a hint and this is a responsibility of
        /// the implementation of the job to properly cancel its execution.
        /// </summary>
        /// <returns><c>True</c> if this job was cancelled, <c>false</c> otherwise.</returns>
        protected bool IsCancelled()
        {
            return _cancelled;
        }

        /// <summary>
        /// Executes this job.
        /// </summary>
        /// <returns>
        /// Job execution result (possibly <c>null</c>). This result will be returned
        /// in <see cref="IComputeJobResult{T}" /> object passed into
        /// <see cref="IComputeTask{TA,T,TR}.OnResult" />
        /// on caller node.
        /// </returns>
        public abstract T Execute();
    }
}
