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

namespace Apache.Ignite.Core.Tests.Deployment
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Task that returns process name.
    /// </summary>
    public class ProcessNameTask : IComputeTask<object, string, string>
    {
        /** <inheritdoc /> */
        public IDictionary<IComputeJob<string>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
        {
            return subgrid.ToDictionary(x => (IComputeJob<string>) new ProcessNameJob {Arg = arg}, x => x);
        }

        /** <inheritdoc /> */
        public ComputeJobResultPolicy OnResult(IComputeJobResult<string> res, IList<IComputeJobResult<string>> rcvd)
        {
            return ComputeJobResultPolicy.Reduce;
        }

        /** <inheritdoc /> */
        public string Reduce(IList<IComputeJobResult<string>> results)
        {
            var ex = results.Select(x => x.Exception).FirstOrDefault(x => x != null);

            if (ex != null)
                throw new IgniteException("Task failed", ex);

            return results.Select(x => x.Data).FirstOrDefault();
        }

        /// <summary>
        /// Job.
        /// </summary>
        private class ProcessNameJob : IComputeJob<string>
        {
            public object Arg { get; set; }

            /** <inheritdoc /> */
            public string Execute()
            {
                return System.Diagnostics.Process.GetCurrentProcess().ProcessName + "_" + Arg;
            }

            /** <inheritdoc /> */
            public void Cancel()
            {
                // No-op.
            }
        }
    }
}
