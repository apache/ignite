/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Compute
{
    using System;

    /// <summary>
    /// Convenience adapter for <see cref="IComputeJob{T}"/> implementations. It provides the following functionality:
    /// <ul>
    /// <li>
    ///      Default implementation of <see cref="GridGain.Compute.IComputeJob{T}.Cancel()"/> method and ability
    ///      to check whether cancellation occurred with <see cref="GridGain.Compute.ComputeJobAdapter{T}.IsCancelled()"/> method.
    /// </li>
    /// <li>
    ///      Ability to set and get job arguments via <see cref="GridGain.Compute.ComputeJobAdapter{T}.SetArguments(object[])"/>
    ///      and <see cref="GridGain.Compute.ComputeJobAdapter{T}.Argument{T}(int)"/> methods.
    /// </li>
    /// </ul>
    /// </summary>
    [Serializable]
    public abstract class ComputeJobAdapter<T> : IComputeJob<T>
    {
        /** Cancelled flag */
        [NonSerialized]
        private volatile bool cancelled;

        /** Arguments. */
        protected object[] args;

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
            this.args = args;
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
            cancelled = true;
        }

        /// <summary>
        /// Sets given arguments.
        /// </summary>
        /// <param name="args">Optional job arguments to set.</param>
        public void SetArguments(params object[] args)
        {
            this.args = args;
        }

        /// <summary>
        /// Sets given arguments.
        /// </summary>
        /// <param name="idx">Index of the argument.</param>
        public TArg Argument<TArg>(int idx)
        {
            if (idx < 0 || idx >= args.Length)
                throw new ArgumentException("Invalid argument index: " + idx);

            return (TArg)args[idx];
        }

        /// <summary>
        /// This method tests whether or not this job was cancelled. This method
        /// is thread-safe and can be called without extra synchronization.
        /// <p/>
        /// This method can be periodically called in <see cref="GridGain.Compute.IComputeJob{T}.Execute()"/> method
        /// implementation to check whether or not this job cancelled. Note that system
        /// calls <see cref="GridGain.Compute.IComputeJob{T}.Cancel()"/> method only as a hint and this is a responsibility of
        /// the implementation of the job to properly cancel its execution.
        /// </summary>
        /// <returns><c>True</c> if this job was cancelled, <c>false</c> otherwise.</returns>
        protected bool IsCancelled()
        {
            return cancelled;
        }

        /// <summary>
        /// Executes this job.
        /// </summary>
        /// <returns>
        /// Job execution result (possibly <c>null</c>). This result will be returned
        /// in <see cref="IComputeJobResult{T}" /> object passed into
        /// <see cref="IComputeTask{A,T,R}.Result" />
        /// on caller node.
        /// </returns>
        public abstract T Execute();
    }
}
