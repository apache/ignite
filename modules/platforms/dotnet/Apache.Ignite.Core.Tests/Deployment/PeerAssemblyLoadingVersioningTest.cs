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

namespace Apache.Ignite.Core.Tests.Deployment
{
    using System;
    using System.CodeDom.Compiler;
    using System.Diagnostics;
    using System.IO;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using NUnit.Framework;

    /// <summary>
    /// Tests assembly versioning: multiple assemblies with same name but different version should be supported.
    /// </summary>
    public class PeerAssemblyLoadingVersioningTest
    {
        private static readonly string TempDir = PathUtils.GetTempDirectoryName();

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            // Copy referenced assemblies.
            foreach (var type in new[] { typeof(Ignition), GetType() })
            {
                var loc = type.Assembly.Location;
                Assert.IsNotNull(loc);
                File.Copy(loc, Path.Combine(TempDir, type.Assembly.GetName().Name + ".dll"));
            }
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Directory.Delete(TempDir, true);
        }

        /// <summary>
        /// Tests that multiple versions of same assembly can be used on remote nodes.
        /// </summary>
        [Test]
        public void TestMultipleVersionsOfSameAssembly()
        {
            using (Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain,
                IgniteInstanceName = "peerDeployTest",
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder {Endpoints = new[] {"127.0.0.1:47500..47502"}},
                    SocketTimeout = TimeSpan.FromSeconds(0.3)
                }
            }))
            {
                RunClientProcess(CompileClientNode(Path.Combine(TempDir, "PeerTest.exe"), "1.0.1.0"));
                RunClientProcess(CompileClientNode(Path.Combine(TempDir, "PeerTest.exe"), "1.0.2.0"));
            }
        }

        /// <summary>
        /// Runs the client process.
        /// </summary>
        private static void RunClientProcess(string exePath)
        {
            var procStart = new ProcessStartInfo
            {
                FileName = exePath,
                CreateNoWindow = true,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };

            var proc = Process.Start(procStart);
            Assert.IsNotNull(proc);

            proc.AttachProcessConsoleReader();

            Assert.IsTrue(proc.WaitForExit(30000));
            Assert.AreEqual(0, proc.ExitCode);

            File.Delete(exePath);
        }

        /// <summary>
        /// Compiles the client node.
        /// </summary>
        private string CompileClientNode(string exePath, string version)
        {
            var parameters = new CompilerParameters
            {
                GenerateExecutable = true,
                OutputAssembly = exePath,
                ReferencedAssemblies =
                {
                    typeof(Ignition).Assembly.Location,
                    GetType().Assembly.Location,
                    "System.dll"
                }
            };

            var src = @"
using System;
using System.Reflection;

using Apache.Ignite.Core;
using Apache.Ignite.Core.Deployment;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Tests;
using Apache.Ignite.Core.Discovery;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

[assembly: AssemblyVersion(""" + version + @""")]

class Program
{
    static void Main(string[] args)
    {
        using (var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(false)) {ClientMode = true, PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain,
                DiscoverySpi = new TcpDiscoverySpi { IpFinder = new TcpDiscoveryStaticIpFinder { Endpoints = new[] { ""127.0.0.1:47500..47502"" } }, SocketTimeout = TimeSpan.FromSeconds(0.3) }
}))
        {
            var res = ignite.GetCompute().Call(new GridNameFunc());
            if (res != ""peerDeployTest_" + version + @""") throw new Exception(""fail: "" + res);
        }
    }
}

public class GridNameFunc : IComputeFunc<string> { public string Invoke() { return Ignition.GetIgnite().Name + ""_"" + GetType().Assembly.GetName().Version; } }
";

            var results = CodeDomProvider.CreateProvider("CSharp").CompileAssemblyFromSource(parameters, src);

            Assert.IsEmpty(results.Errors);

            return exePath;
        }
    }
}
