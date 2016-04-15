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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Common;
    using Microsoft.CSharp;
    using NUnit.Framework;

    /// <summary>
    /// Dll loading test.
    /// </summary>
    public class LoadDllTest
    {
        /// <summary>
        /// 
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();
        }

        /// <summary>
        /// 
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadFromGac()
        {
            Assert.False(IsLoaded("System.Data.Linq"));

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid3.xml",
                Assemblies =
                    new List<string>
                    {
                        "System.Data.Linq,Culture=neutral,Version=1.0.0.0,PublicKeyToken=b77a5c561934e089"
                    },
                JvmClasspath = TestUtils.CreateTestClasspath()
            };


            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("System.Data.Linq"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadFromCurrentDir()
        {
            Assert.False(IsLoaded("testDll"));

            GenerateDll("testDll.dll");

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid3.xml",
                Assemblies = new List<string> {"testDll.dll"},
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("testDll"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadAllDllInDir()
        {
            var dirInfo = Directory.CreateDirectory(Path.GetTempPath() + "/testDlls");
            
            Assert.False(IsLoaded("dllFromDir1"));
            Assert.False(IsLoaded("dllFromDir2"));

            GenerateDll(dirInfo.FullName + "/dllFromDir1.dll");
            GenerateDll(dirInfo.FullName + "/dllFromDir2.dll");
            File.WriteAllText(dirInfo.FullName + "/notADll.txt", "notADll");

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid3.xml",
                Assemblies = new List<string> {dirInfo.FullName},
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("dllFromDir1"));
            Assert.True(IsLoaded("dllFromDir2"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadFromCurrentDirByName()
        {
            Assert.False(IsLoaded("testDllByName"));

            GenerateDll("testDllByName.dll");

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid3.xml",
                Assemblies = new List<string> {"testDllByName"},
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("testDllByName"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadByAbsoluteUri()
        {
            var dllPath = Path.GetTempPath() + "/tempDll.dll";
            Assert.False(IsLoaded("tempDll"));

            GenerateDll(dllPath);

            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid3.xml",
                Assemblies = new List<string> {dllPath},
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            var grid = Ignition.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("tempDll"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadUnexistingLibrary()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\start-test-grid3.xml",
                Assemblies = new List<string> {"unexistingAssembly.820482.dll"},
                JvmClasspath = TestUtils.CreateTestClasspath()
            };

            try
            {
                Ignition.Start(cfg);

                Assert.Fail("Grid has been started with broken configuration.");
            }
            catch (IgniteException)
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="outputPath"></param>
        private void GenerateDll(string outputPath)
        {
            var codeProvider = new CSharpCodeProvider();

#pragma warning disable 0618

            var icc = codeProvider.CreateCompiler();

#pragma warning restore 0618

            var parameters = new CompilerParameters
            {
                GenerateExecutable = false,
                OutputAssembly = outputPath
            };

            var src = "namespace Apache.Ignite.Client.Test { public class Foo {}}";

            var results = icc.CompileAssemblyFromSource(parameters, src);

            Assert.False(results.Errors.HasErrors);
        }

        /// <summary>
        /// Determines whether the specified assembly is loaded.
        /// </summary>
        /// <param name="asmName">Name of the assembly.</param>
        private static bool IsLoaded(string asmName)
        {
            return AppDomain.CurrentDomain.GetAssemblies().Any(a => a.GetName().Name == asmName);
        }
    }
}
