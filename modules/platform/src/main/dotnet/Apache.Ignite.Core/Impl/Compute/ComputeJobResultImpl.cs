/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Compute
{
    using System;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Job result implementation.
    /// </summary>
    internal class ComputeJobResultImpl : IComputeJobResult<object>
    {
        /** Data. */
        private readonly object data;

        /** Exception. */
        private readonly Exception err;

        /** Backing job. */
        private readonly IComputeJob job;

        /** Node ID. */
        private readonly Guid nodeId;

        /** Cancel flag. */
        private readonly bool cancelled;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="data">Data.</param>
        /// <param name="err">Exception.</param>
        /// <param name="job">Backing job.</param>
        /// <param name="nodeId">Node ID.</param>
        /// <param name="cancelled">Cancel flag.</param>
        public ComputeJobResultImpl(object data, Exception err, IComputeJob job, Guid nodeId, bool cancelled)
        {
            this.data = data;
            this.err = err;
            this.job = job;
            this.nodeId = nodeId;
            this.cancelled = cancelled;
        }

        /** <inheritDoc /> */
        public object Data()
        {
            return data;
        }

        /** <inheritDoc /> */
        public Exception Exception()
        {
            return err;
        }

        /** <inheritDoc /> */
        public IComputeJob<object> Job()
        {
            return job;
        }

        /** <inheritDoc /> */
        public Guid NodeId
        {
            get
            {
                return nodeId;
            }
        }

        /** <inheritDoc /> */
        public bool Cancelled
        {
            get 
            { 
                return cancelled; 
            }
        }
    }
}
