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

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using Apache.Ignite.Core.Discovery;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;

    /// <summary>
    /// Test utils.
    /// </summary>
    public static class TestUtil
    {
        /// <summary>
        /// Gets the local discovery spi.
        /// </summary>
        public static IDiscoverySpi GetLocalDiscoverySpi()
        {
            return new TcpDiscoverySpi
            {
                IpFinder = new TcpDiscoveryStaticIpFinder
                {
                    Endpoints = new[] {"127.0.0.1:47500..47503"}
                }
            };
        }

        /// <summary>
        /// Attaches the process console reader.
        /// </summary>
        public static void AttachProcessConsoleReader(Process process)
        {
            Attach(process, process.StandardOutput, false);
            Attach(process, process.StandardError, true);
        }

        /// <summary>
        /// Attach output reader to the process.
        /// </summary>
        private static void Attach(Process proc, TextReader reader, bool err)
        {
            new Thread(() =>
            {
                while (!proc.HasExited)
                {
                    Console.WriteLine(err ? ">>> {0} ERR: {1}" : ">>> {0} OUT: {1}", proc.Id, reader.ReadLine());
                }
            })
            {
                IsBackground = true
            }.Start();
        }
    }
}
