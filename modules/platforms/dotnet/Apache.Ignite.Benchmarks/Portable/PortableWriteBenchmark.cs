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

namespace GridGain.Client.Benchmark.Portable
{
    using System.Collections.Generic;

    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;
    using GridGain.Client.Benchmark.Model;

    /// <summary>
    /// Portable write benchmark.
    /// </summary>
    class PortableWriteBenchmark : GridClientAbstractBenchmark
    {
        /** Marshaller. */
        private readonly PortableMarshaller marsh;

        private readonly PlatformMemoryManager memMgr = new PlatformMemoryManager(1024);

        /** Pre-allocated addess. */
        private readonly Address a = GridClientBenchmarkUtils.RandomAddress();

        public PortableWriteBenchmark()
        {
            var addrCfg = new PortableTypeConfiguration(typeof (Address));

            addrCfg.MetadataEnabled = false;

            PortableConfiguration cfg = new PortableConfiguration
            {
                TypeConfigurations = new List<PortableTypeConfiguration>
                {
                    addrCfg
                }
            };

            marsh = new PortableMarshaller(cfg);
        }

        /// <summary>
        /// Populate descriptors.
        /// </summary>
        /// <param name="descs">Descriptors.</param>
        protected override void Descriptors(ICollection<GridClientBenchmarkOperationDescriptor> descs)
        {
            descs.Add(GridClientBenchmarkOperationDescriptor.Create("WriteAddress", WriteAddress, 1));
        }

        /// <summary>
        /// Write address.
        /// </summary>
        /// <param name="state">State.</param>
        public void WriteAddress(GridClientBenchmarkState state)
        {
            IPlatformMemory mem = memMgr.Allocate();

            try
            {
                IPortableStream stream = mem.Stream();

                marsh.StartMarshal(stream).Write(a);
            }
            finally
            {
                mem.Release();
            }
        }
    }
}
