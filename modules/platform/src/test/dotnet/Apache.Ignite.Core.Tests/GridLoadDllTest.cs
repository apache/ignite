/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.CodeDom.Compiler;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using Apache.Ignite.Core.Common;
    using Microsoft.CSharp;
    using NUnit.Framework;

    /// <summary>
    /// 
    /// </summary>
    public class GridLoadDllTest
    {
        /// <summary>
        /// 
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            GridTestUtils.KillProcesses();
        }

        /// <summary>
        /// 
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            GridFactory.StopAll(true);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadFromGAC()
        {
            Assert.False(IsLoaded("System.Data.Linq"));

            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid3.xml";
            cfg.Assemblies = new List<string> { "System.Data.Linq,Culture=neutral,Version=1.0.0.0,PublicKeyToken=b77a5c561934e089" };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = GridFactory.Start(cfg);

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

            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid3.xml";
            cfg.Assemblies = new List<string> { "testDll.dll" };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = GridFactory.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("testDll"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadAllDllInDir()
        {
            DirectoryInfo dirInfo = Directory.CreateDirectory(Path.GetTempPath() + "/testDlls");
            
            Assert.False(IsLoaded("dllFromDir1"));
            Assert.False(IsLoaded("dllFromDir2"));

            GenerateDll(dirInfo.FullName + "/dllFromDir1.dll");
            GenerateDll(dirInfo.FullName + "/dllFromDir2.dll");
            File.WriteAllText(dirInfo.FullName + "/notADll.txt", "notADll");

            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid3.xml";
            cfg.Assemblies = new List<string> { dirInfo.FullName };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = GridFactory.Start(cfg);

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

            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid3.xml";
            cfg.Assemblies = new List<string> { "testDllByName" };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = GridFactory.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("testDllByName"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadByAbsoluteUri()
        {
            string dllPath = Path.GetTempPath() + "/tempDll.dll";
            Assert.False(IsLoaded("tempDll"));

            GenerateDll(dllPath);

            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid3.xml";
            cfg.Assemblies = new List<string> { dllPath };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            IIgnite grid = GridFactory.Start(cfg);

            Assert.IsNotNull(grid);

            Assert.True(IsLoaded("tempDll"));
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void TestLoadUnexistingLibrary()
        {
            GridConfiguration cfg = new GridConfiguration();

            cfg.SpringConfigUrl = "config\\start-test-grid3.xml";
            cfg.Assemblies = new List<string> { "unexistingAssembly.820482.dll" };
            cfg.JvmClasspath = GridTestUtils.CreateTestClasspath();

            try
            {
                IIgnite grid = GridFactory.Start(cfg);

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
            CSharpCodeProvider codeProvider = new CSharpCodeProvider();

#pragma warning disable 0618

            ICodeCompiler icc = codeProvider.CreateCompiler();

#pragma warning restore 0618

            System.CodeDom.Compiler.CompilerParameters parameters = new CompilerParameters();
            parameters.GenerateExecutable = false;
            parameters.OutputAssembly = outputPath;

            string src = "namespace GridGain.Client.Test { public class Foo {}}";

            CompilerResults results = icc.CompileAssemblyFromSource(parameters, src);

            Assert.False(results.Errors.HasErrors);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static bool IsLoaded(string name)
        {
            foreach (Assembly a in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (a.GetName().Name == name)
                    return true;
            }

            return false;
        }
    }
}
