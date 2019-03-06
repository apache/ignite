/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Benchmarks.Interop
{
    using System.Collections.Generic;
    using System.Net;
    using Apache.Ignite.Benchmarks.Model;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;

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
                BinaryConfiguration = GetBinaryConfiguration(),
                JvmOptions = new List<string>
                {
                    "-Xms2g",
                    "-Xmx2g",
                    "-DIGNITE_QUIET=false",
                    "-DIGNITE_NO_SHUTDOWN_HOOK=true"
                },
                JvmClasspath = Classpath ?? Core.Impl.Common.Classpath.CreateClasspath(forceTestClasspath: true),
                JvmDllPath = DllPath,
                SpringConfigUrl = ConfigPath
            };

            Node = Ignition.Start(cfg);
        }

        /// <summary>
        /// Get binary configuration.
        /// </summary>
        /// <returns>Gets binary configuration.</returns>
        private static BinaryConfiguration GetBinaryConfiguration()
        {
            return new BinaryConfiguration
            {
                TypeConfigurations = new List<BinaryTypeConfiguration>
                {
                    new BinaryTypeConfiguration(typeof (Address)),
                    new BinaryTypeConfiguration(typeof (Company)),
                    new BinaryTypeConfiguration(typeof (Employee)),
                    new BinaryTypeConfiguration(typeof (MyClosure)),
                    new BinaryTypeConfiguration(typeof (MyJob))
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

        /// <summary>
        /// Gets the client.
        /// </summary>
        protected IIgniteClient GetClient()
        {
            return Ignition.StartClient(GetClientConfiguration());
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        private IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration
            {
                Endpoints = new[] {IPAddress.Loopback.ToString()}
            };
        }
    }
}
