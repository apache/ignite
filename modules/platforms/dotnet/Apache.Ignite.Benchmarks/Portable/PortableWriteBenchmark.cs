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

namespace Apache.Ignite.Benchmarks.Portable
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable write benchmark.
    /// </summary>
    internal class PortableWriteBenchmark : BenchmarkBase
    {
        /** Marshaller. */
        private readonly PortableMarshaller _marsh;

        private readonly PlatformMemoryManager _memMgr = new PlatformMemoryManager(1024);

        /** Pre-allocated addess. */
        private readonly Address _address = BenchmarkUtils.RandomAddress();

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableWriteBenchmark"/> class.
        /// </summary>
        public PortableWriteBenchmark()
        {
            _marsh = new PortableMarshaller(new PortableConfiguration
            {
                TypeConfigurations = new List<PortableTypeConfiguration>
                {
                    new PortableTypeConfiguration(typeof (Address)) {MetadataEnabled = false}
                }
            });
        }

        /// <summary>
        /// Populate descriptors.
        /// </summary>
        /// <param name="descs">Descriptors.</param>
        protected override void Descriptors(ICollection<BenchmarkOperationDescriptor> descs)
        {
            descs.Add(BenchmarkOperationDescriptor.Create("WriteAddress", WriteAddress, 1));
        }

        /// <summary>
        /// Write address.
        /// </summary>
        /// <param name="state">State.</param>
        private void WriteAddress(BenchmarkState state)
        {
            var mem = _memMgr.Allocate();

            try
            {
                var stream = mem.Stream();

                _marsh.StartMarshal(stream).Write(_address);
            }
            finally
            {
                mem.Release();
            }
        }
    }
}
