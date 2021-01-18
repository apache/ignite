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

namespace Apache.Ignite.Core.Tests.Examples
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using NUnit.Framework;

    /// <summary>
    /// Represents an Example to be tested.
    /// </summary>
    public class Example
    {
        /** All projects. */
        public static readonly Example[] AllProjects = GetExamples().ToArray();

        /** All examples. */
        public static readonly Example[] AllExamples = AllProjects.Where(p => p.Name != "ServerNode").ToArray();

        /** Method invoke flags. */
        private const BindingFlags InvokeFlags = BindingFlags.Static | BindingFlags.Public | BindingFlags.InvokeMethod;

        /** Example name. */
        public string Name { get; }

        /** Project file. */
        public string ProjectFile { get; }

        /** Assembly path. */
        public string AssemblyFile { get; }

        /** Whether this is a thin client example (needs a server node). */
        public bool IsThin => ProjectFile.Contains("Thin");

        /** Example source code. */
        public string SourceCode { get; }

        /** Whether this example runs in thick client mode. */
        public bool IsClient { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="Example"/> class.
        /// </summary>
        private Example(string name, string projectFile, string assemblyFile, string sourceCode)
        {
            Name = name;
            ProjectFile = projectFile;
            AssemblyFile = assemblyFile;
            SourceCode = sourceCode;
            IsClient = sourceCode.Contains("GetClientNodeConfiguration");
        }

        /// <summary>
        /// Runs this example.
        /// </summary>
        public void Run()
        {
            try
            {
                Assert.IsTrue(File.Exists(AssemblyFile),
                    $"Assembly not found: {AssemblyFile}. " +
                    "Make sure to build IgniteExamples.sln. This usually happens as part of build.ps1 execution.");

                var assembly = Assembly.LoadFrom(AssemblyFile);

                var programType = assembly.GetTypes().SingleOrDefault(t => t.Name == "Program");
                Assert.IsNotNull(programType, $"Assembly {AssemblyFile} does not have Program class.");

                programType.InvokeMember("Main", InvokeFlags, null, null, null);
            }
            catch (TargetInvocationException ex)
            {
                // Each example has a ReadKey at the end, which throws an exception in test environment.
                if (ex.InnerException is InvalidOperationException inner &&
                    inner.Message.StartsWith("Cannot read keys"))
                {
                    return;
                }

                throw;
            }

            throw new Exception("ReadKey is missing at the end of the example.");
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            // This will be displayed by the test runner in CI and IDE.
            return Name;
        }

        /// <summary>
        /// Gets all examples.
        /// </summary>
        private static IEnumerable<Example> GetExamples()
        {
            var projFiles = Directory
                .GetFiles(ExamplePaths.SourcesPath, "*.csproj", SearchOption.AllDirectories)
                .Where(x => !x.EndsWith("Shared.csproj")).ToArray();

            Assert.IsTrue(projFiles.Any());

            return projFiles
                .Select(projFile =>
                {
                    var name = Path.GetFileNameWithoutExtension(projFile);
                    var path = Path.GetDirectoryName(projFile);
                    var asmFile = Path.Combine(path, "bin", "Debug", "netcoreapp2.1", $"{name}.dll");

                    var sourceFile = Path.Combine(path, "Program.cs");
                    var sourceCode = File.ReadAllText(sourceFile);

                    return new Example(name, projFile, asmFile, sourceCode);
                });
        }
    }
}
