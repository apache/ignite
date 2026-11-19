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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Binary;
    using static Apache.Ignite.Core.Impl.Common.IgniteArgumentCheck;
    using static ComputeTaskUtils;

    /// <summary>
    /// Implements <see cref="IComputeTaskSession"/> by delegating the implementation to the Java side.
    /// </summary>
    internal class ComputeTaskSession : PlatformTargetAdapter, IComputeTaskSession, IComputeTaskContinuousMapper
    {
        /// <summary>
        /// This session's task
        /// </summary>
        private readonly IComputeTaskHolder _task;

        /// <summary>
        /// Operation codes
        /// </summary>
        private enum Op
        {
            GetAttribute = 1,
            SetAttributes = 2,
            SendJob = 3,
            SendJobRandom = 4
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeTaskSession"/> class.
        /// </summary>
        public ComputeTaskSession(IPlatformTargetInternal target, IComputeTaskHolder task) : base(target)
        {
            _task = task;
        }

        /// <inheritdoc />
        public TV GetAttribute<TK, TV>(TK key) =>
            DoOutInOp<TV>((int) Op.GetAttribute, w => w.Write(key));

        /// <inheritdoc />
        public void SetAttributes<TK, TV>(params KeyValuePair<TK, TV>[] attrs) =>
            DoOutOp((int) Op.SetAttributes, writer => writer.WriteDictionary(attrs));

        /// <inheritdoc />
        public void Send<TRes>(IComputeJob<TRes> job, IClusterNode node) =>
            Send(new Dictionary<IComputeJob<TRes>, IClusterNode> { { job, node } });

        /// <inheritdoc />
        public void Send<TRes>(IDictionary<IComputeJob<TRes>, IClusterNode> mappedJobs)
        {
            NotNull(mappedJobs, "mappedJobs");

            DoOutOp((int)Op.SendJob, writer =>
            {
                var jobHandles = WriteJobs(writer, mappedJobs);
                _task.AddJobs(jobHandles);
            });
        }

        /// <inheritdoc />
        public void Send<TRes>(IComputeJob<TRes> job) =>
            Send(new List<IComputeJob<TRes>> { job });

        /// <inheritdoc />
        public void Send<TRes>(ICollection<IComputeJob<TRes>> jobs)
        {
            NotNull(jobs, "jobs");

            DoOutOp((int)Op.SendJobRandom, writer =>
            {
                var jobHandles = WriteJobs(writer, jobs);
                _task.AddJobs(jobHandles);
            });
        }
    }
}