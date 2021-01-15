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
    using NUnit.Framework;

    /// <summary>
    /// Represents an Example to be tested.
    /// </summary>
    public class Example
    {
        /** Name */
        public string Name { get; }

        /** Project file */
        public string ProjectFile { get; }

        /** Assembly path */
        public string AssemblyFile { get; }

        public Example(string name, string projectFile, string assemblyFile)
        {
            Name = name;
            ProjectFile = projectFile;
            AssemblyFile = assemblyFile;
        }

        /// <summary>
        /// Runs this example.
        /// </summary>
        public void Run()
        {
            try
            {
                Assert.IsTrue(File.Exists(AssemblyFile), $"Assembly not found: {AssemblyFile}");

                // TODO
            }
            catch (InvalidOperationException ex)
            {
                // Each example has a ReadKey at the end, which throws an exception in test environment.
                if (ex.Message != "Cannot read keys when either application does not have a console or " +
                    "when console input has been redirected from a file. Try Console.Read.")
                {
                    throw;
                }

                return;
            }

            throw new Exception("ReadKey missing at the end of the example.");
        }

        /// <summary>
        /// Gets all examples.
        /// </summary>
        public static IEnumerable<Example> GetExamples()
        {
            var projFiles = Directory.GetFiles(ExamplePaths.ExamplesSourcePath, "*.csproj", SearchOption.AllDirectories)
                .Where(x => !x.EndsWith("Shared.csproj") && !x.EndsWith("ServerNode.csproj")).ToArray();

            Assert.IsTrue(projFiles.Any());

            return projFiles
                .Select(projFile =>
                {
                    var name = Path.GetFileNameWithoutExtension(projFile);
                    var path = Path.GetDirectoryName(projFile);
                    var asmFile = Path.Combine(path, "bin", "debug", "netcoreapp2.1", $"{name}.dll");

                    return new Example(name, projFile, asmFile);
                });
        }

        /** <inheritdoc /> */
        public override string ToString()
        {
            // This will be displayed in TeamCity and R# test runner
            return Name;
        }
    }
}
