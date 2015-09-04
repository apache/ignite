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
        private readonly object _data;

        /** Exception. */
        private readonly Exception _err;

        /** Backing job. */
        private readonly IComputeJob _job;

        /** Node ID. */
        private readonly Guid _nodeId;

        /** Cancel flag. */
        private readonly bool _cancelled;

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
            _data = data;
            _err = err;
            _job = job;
            _nodeId = nodeId;
            _cancelled = cancelled;
        }

        /** <inheritDoc /> */
        public object Data()
        {
            return _data;
        }

        /** <inheritDoc /> */
        public Exception Exception()
        {
            return _err;
        }

        /** <inheritDoc /> */
        public IComputeJob<object> Job()
        {
            return _job;
        }

        /** <inheritDoc /> */
        public Guid NodeId
        {
            get
            {
                return _nodeId;
            }
        }

        /** <inheritDoc /> */
        public bool Cancelled
        {
            get 
            { 
                return _cancelled; 
            }
        }
    }
}
