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
