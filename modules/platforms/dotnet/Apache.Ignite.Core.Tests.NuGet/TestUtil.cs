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
