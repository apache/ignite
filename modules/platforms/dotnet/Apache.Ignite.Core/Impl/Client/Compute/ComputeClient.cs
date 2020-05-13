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

namespace Apache.Ignite.Core.Impl.Client.Compute
{
    using System;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client.Compute;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Compute client.
    /// </summary>
    internal class ComputeClient : IComputeClient
    {
        /** */
        private readonly IgniteClient _ignite;

        /// <summary>
        /// Initializes a new instance of <see cref="ComputeClient"/>.
        /// </summary>
        /// <param name="ignite"></param>
        internal ComputeClient(IgniteClient ignite)
        {
            // TODO: Projection?
            _ignite = ignite;
        }

        /** <inheritdoc /> */
        public TRes ExecuteJavaTask<TRes>(string taskName, object taskArg)
        {
            IgniteArgumentCheck.NotNullOrEmpty(taskName, "taskName");

            var taskId = _ignite.Socket.DoOutInOp(ClientOp.ComputeTaskExecute, ctx =>
                {
                    var w = ctx.Writer;

                    w.WriteInt(0); // TODO: Projection.
                    w.WriteByte(0); // TODO: Flags
                    w.WriteLong(0); // TODO: Timeout
                    w.WriteString(taskName);
                    w.WriteObject(taskArg);
                },
                ctx => ctx.Stream.ReadLong());

            // TODO: Wait for task completion.
            return default(TRes);
        }

        /** <inheritdoc /> */
        public Task<TRes> ExecuteJavaTaskAsync<TRes>(string taskName, object taskArg)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public IComputeClient WithTimeout(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoFailover()
        {
            throw new NotImplementedException();
        }

        /** <inheritdoc /> */
        public IComputeClient WithNoResultCache()
        {
            throw new NotImplementedException();
        }
    }
}