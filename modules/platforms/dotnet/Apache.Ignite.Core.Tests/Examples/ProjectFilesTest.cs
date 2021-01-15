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
    using System.IO;
    using NUnit.Framework;

    /// <summary>
    /// Tests project files.
    /// </summary>
    public class ProjectFilesTest
    {
        /** */
        private static readonly Example[] Examples = Example.AllExamples;

        /** */
        private static readonly string ExamplesSlnText = File.ReadAllText(ExamplePaths.SlnFile);

        /** */
        private static readonly string LaunchJsonText = File.ReadAllText(ExamplePaths.LaunchJsonFile);

        /// <summary>
        /// Checks csproj files.
        /// </summary>
        [Test, TestCaseSource(nameof(Examples))]
        public void TestCsprojFiles(Example example)
        {
            // TODO:
            // * All projects have correct namespaces
            // * All examples have Thin and Thick variants when possible
            Assert.IsTrue(File.Exists(example.ProjectFile), $"File.Exists({example.ProjectFile})");

            var text = File.ReadAllText(example.ProjectFile);

            StringAssert.Contains("<OutputType>Exe</OutputType>", text);
            StringAssert.Contains("<TargetFramework>netcoreapp2.1</TargetFramework>", text);
            StringAssert.Contains("<RootNamespace>IgniteExamples.", text);
            StringAssert.Contains("<ProjectReference Include=\"..\\..\\..\\Shared\\Shared.csproj", text);
            StringAssert.Contains($"{example.Name}.csproj", ExamplesSlnText);
            StringAssert.Contains($"{example.Name}.dll", LaunchJsonText);
        }
    }
}
