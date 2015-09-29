/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark.Portable
{
    using System.Collections.Generic;

    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable.IO;

    using GridGain.Client.Benchmark.Model;
    using GridGain.Impl.Memory;
    using GridGain.Impl.Portable;
    using GridGain.Portable;

    /// <summary>
    /// Portable write benchmark.
    /// </summary>
    class PortableWriteBenchmark : GridClientAbstractBenchmark
    {
        /** Marshaller. */
        private readonly PortableMarshaller marsh;

        private readonly InteropMemoryManager memMgr = new InteropMemoryManager(1024);

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
