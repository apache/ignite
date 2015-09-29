/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Benchmark.Interop
{
    using System.Collections.Generic;
    using GridGain.Client.Benchmark.Model;
    using GridGain.Impl;
    using GridGain.Portable;
    using BU = GridClientBenchmarkUtils;

    /// <summary>
    /// Base class for all interop benchmarks.
    /// </summary>
    internal abstract class GridClientAbstractInteropBenchmark : GridClientAbstractBenchmark
    {
        /** Default dataset. */
        private const int DFLT_DATASET = 100000;

        /** Default payload. */
        private const int DFLT_PAYLOAD = 128;

        /** Native node. */
        protected IGrid node;

        /** Employees. */
        protected Employee[] emps;

        /// <summary>
        /// Constructor.
        /// </summary>
        protected GridClientAbstractInteropBenchmark()
        {
            Dataset = DFLT_DATASET;
            Payload = DFLT_PAYLOAD;
        }

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            emps = new Employee[Dataset];

            for (int i = 0; i < emps.Length; i++)
                emps[i] = BU.RandomEmployee(Payload);

            GridConfiguration cfg = new GridConfiguration
            {
                PortableConfiguration = GetPortableConfiguration(),
                JvmOptions = new List<string>
                {
                    "-Xms2g",
                    "-Xmx2g",
                    "-DIGNITE_QUIET=false",
                    "-DIGNITE_NO_SHUTDOWN_HOOK=true"
                },
                JvmClasspath = Classpath ?? GridManager.CreateClasspath(),
                JvmDllPath = DllPath,
                SpringConfigUrl = ConfigPath
            };

            node = GridFactory.Start(cfg);
        }

        /// <summary>
        /// Get portable configuration.
        /// </summary>
        /// <returns>Portable configuration.</returns>
        protected PortableConfiguration GetPortableConfiguration()
        {
            return new PortableConfiguration
            {
                TypeConfigurations = new List<PortableTypeConfiguration>
                {
                    new PortableTypeConfiguration(typeof (Address)),
                    new PortableTypeConfiguration(typeof (Company)),
                    new PortableTypeConfiguration(typeof (Employee)),
                    new PortableTypeConfiguration(typeof (MyClosure)),
                    new PortableTypeConfiguration(typeof (MyJob))
                },
                DefaultMetadataEnabled = false
            };
        }

        /// <summary>
        /// Classpath.
        /// </summary>
        public string Classpath
        {
            get;
            set;
        }

        /// <summary>
        /// Path to JVM.DLL.
        /// </summary>
        public string DllPath
        {
            get;
            set;
        }

        /// <summary>
        /// Path to XML configuration.
        /// </summary>
        public string ConfigPath
        {
            get;
            set;
        }

        /// <summary>
        /// Data set size.
        /// </summary>
        public int Dataset
        {
            get;
            set;
        }

        /// <summary>
        /// Payload.
        /// </summary>
        public int Payload
        {
            get;
            set;
        }
    }
}
