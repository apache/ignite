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
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Resource;
    using static IgniteUtils;

    /// <summary>
    /// System job which wraps over <c>Func</c>.
    /// </summary>
    internal class ComputeOutFuncJob : IComputeJob, IComputeResourceInjector, IBinaryWriteAware
    {
        /** Closure. */
        private readonly IComputeOutFunc _clo;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="clo">Closure.</param>
        public ComputeOutFuncJob(IComputeOutFunc clo)
        {
            _clo = clo;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            return _clo.Invoke();
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            throw new NotSupportedException("Func job cannot be cancelled.");
        }

        /** <inheritDoc /> */
        public void Inject(IIgniteInternal grid)
        {
            ResourceProcessor.Inject(_clo, grid);
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter) writer.GetRawWriter();

            writer0.WriteObjectDetached(_clo);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeOutFuncJob" /> class.
        /// </summary>
        public ComputeOutFuncJob(IBinaryRawReader reader)
        {
            _clo = reader.ReadObject<IComputeOutFunc>();
        }

        /** <inheritDoc /> */ 
        public string GetName()
        {
            return _clo.GetName();
        } 
    }
}
