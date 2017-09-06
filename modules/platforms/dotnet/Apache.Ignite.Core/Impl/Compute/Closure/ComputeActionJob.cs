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

namespace Apache.Ignite.Core.Impl.Compute.Closure
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Deployment;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// System job which wraps over <c>Action</c>.
    /// </summary>
    internal class ComputeActionJob : IComputeJob, IComputeResourceInjector, IBinaryWriteAware
    {
        /** Closure. */
        private readonly IComputeAction _action;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="action">Action.</param>
        public ComputeActionJob(IComputeAction action)
        {
            _action = action;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            _action.Invoke();
            
            return null;
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            throw new NotSupportedException("Func job cannot be cancelled.");
        }

        /** <inheritDoc /> */
        public void Inject(IIgniteInternal grid)
        {
            ResourceProcessor.Inject(_action, grid);
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter) writer.GetRawWriter();

            writer0.WriteWithPeerDeployment(_action);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeActionJob"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeActionJob(IBinaryRawReader reader)
        {
            _action = (IComputeAction) reader.ReadObject<object>();
        }
    }
}