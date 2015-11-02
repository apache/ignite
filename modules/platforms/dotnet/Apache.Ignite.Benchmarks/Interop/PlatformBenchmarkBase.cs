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

namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Base class for all platform benchmarks.
    /// </summary>
    internal abstract class PlatformBenchmarkBase : BenchmarkBase
    {
        /** Default dataset. */
        private const int DfltDataset = 100000;

        /** Default payload. */
        private const int DfltPayload = 128;

        /** Native node. */
        protected IIgnite Node;

        /** Employees. */
        protected Employee[] Emps;

        /// <summary>
        /// Constructor.
        /// </summary>
        protected PlatformBenchmarkBase()
        {
            Dataset = DfltDataset;
            Payload = DfltPayload;
        }

        /** <inheritDoc /> */
        protected override void OnStarted()
        {
            Emps = new Employee[Dataset];

            for (var i = 0; i < Emps.Length; i++)
                Emps[i] = BenchmarkUtils.GetRandomEmployee(Payload);

            var cfg = new IgniteConfiguration
            {
                PortableConfiguration = GetPortableConfiguration(),
                JvmOptions = new List<string>
                {
                    "-Xms2g",
                    "-Xmx2g",
                    "-DIGNITE_QUIET=false",
                    "-DIGNITE_NO_SHUTDOWN_HOOK=true"
                },
                JvmClasspath = Classpath ?? Core.Impl.Common.Classpath.CreateClasspath(),
                JvmDllPath = DllPath,
                SpringConfigUrl = ConfigPath
            };

            Node = Ignition.Start(cfg);
        }

        /// <summary>
        /// Get portable configuration.
        /// </summary>
        /// <returns>Portable configuration.</returns>
        private static PortableConfiguration GetPortableConfiguration()
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
                }
            };
        }

        /// <summary>
        /// Classpath.
        /// </summary>
        public string Classpath { get; set; }

        /// <summary>
        /// Path to JVM.DLL.
        /// </summary>
        public string DllPath { get; set; }

        /// <summary>
        /// Path to XML configuration.
        /// </summary>
        public string ConfigPath { get; set; }

        /// <summary>
        /// Data set size.
        /// </summary>
        public int Dataset { get; set; }

        /// <summary>
        /// Payload.
        /// </summary>
        public int Payload { get; set; }
    }
}
