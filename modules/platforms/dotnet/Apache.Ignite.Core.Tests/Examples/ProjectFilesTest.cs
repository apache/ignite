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
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using NUnit.Framework;

    /// <summary>
    /// Tests project files.
    /// </summary>
    public class ProjectFilesTest
    {
        /** */
        private const string WorkspaceFolder = "${workspaceFolder}";

        /** */
        private static readonly Example[] Examples = Example.AllExamples;

        /** */
        private static readonly string ExamplesSlnText = File.ReadAllText(ExamplePaths.SlnFile);

        /** */
        private static readonly string LaunchJsonText = File.ReadAllText(ExamplePaths.LaunchJsonFile);

        /** */
        private static readonly string TargetFramework = ExamplePaths.GetTargetFramework(ExamplePaths.SharedProjFile);

        /** */
        private static readonly string[] ThickOnlyExamples = {
            "NearCache", "DataStreamer", "MultiTieredCache", "QueryFullText", "Store", "EntryProcessor",
             "TransactionDeadlockDetection", "Func", "PeerAssemblyLoading", "Task", "AtomicLong",
             "AtomicReference", "AtomicSequence", "ClientReconnect", "Events", "Lifecycle", "Messaging"
        };

        /// <summary>
        /// Checks csproj files.
        /// </summary>
        [Test, TestCaseSource(nameof(Examples))]
        public void TestCsprojFiles(Example example)
        {
            Assert.IsTrue(File.Exists(example.ProjectFile), $"File.Exists({example.ProjectFile})");

            var text = File.ReadAllText(example.ProjectFile);

            StringAssert.Contains("<OutputType>Exe</OutputType>", text);
            StringAssert.Contains($"<TargetFramework>{TargetFramework}</TargetFramework>", text);
            StringAssert.Contains("<ProjectReference Include=\"..\\..\\..\\Shared\\Shared.csproj", text);
            StringAssert.Contains($"{example.Name}.csproj", ExamplesSlnText);
            StringAssert.Contains($"{example.Name}.dll", LaunchJsonText);

            StringAssert.DoesNotContain("TODO", example.SourceCode);
            StringAssert.DoesNotContain("STAThread", example.SourceCode);
            StringAssert.Contains("public static class Program", example.SourceCode);
            StringAssert.Contains("public static void Main()", example.SourceCode);

            var expectedRootNamespace = example.ProjectFile
                .Replace(ExamplePaths.SourcesPath, string.Empty)
                .Replace(Path.DirectorySeparatorChar, '.')
                .Replace("." + Path.GetFileName(example.ProjectFile), string.Empty);

            StringAssert.Contains($"<RootNamespace>Apache.Ignite.Examples{expectedRootNamespace}</RootNamespace>", text);

            if (!example.IsThin && !ThickOnlyExamples.Contains(example.Name))
            {
                var thinCounterpart = example.Name + "Thin";

                Assert.IsTrue(
                    Examples.Any(e => e.Name == thinCounterpart),
                    $"Missing thin client example: {thinCounterpart}");
            }
        }

        /// <summary>
        /// Checks VS Code files.
        /// </summary>
        [Test]
        public void TestVsCodeFiles()
        {
            var tasksText = File.ReadAllText(ExamplePaths.TasksJsonFile);
            StringAssert.Contains(Path.GetFileName(ExamplePaths.SlnFile), tasksText);

            var configsCount = Regex.Matches(LaunchJsonText, "\"name\":").Count;
            var expectedCount = Example.AllExamples.Length + 1; // +1 for ServerNode project

            Assert.AreEqual(expectedCount, configsCount);
        }

        /// <summary>
        /// Updates launch.json with all examples.
        /// <para />
        /// Run this test to update the file when necessary.
        /// This test is not won't be run automatically because of <see cref="ExplicitAttribute"/>.
        /// </summary>
        [Test]
        [Explicit]
        public void UpdateLaunchJson()
        {
            var launch = new Launch
            {
                Configurations = Example.AllProjects.Select(e => new LaunchConfig
                {
                    Name = e.Name,
                    Program = e.AssemblyFile.Replace(ExamplePaths.SourcesPath, WorkspaceFolder),
                    Cwd = Path.GetDirectoryName(e.ProjectFile).Replace(ExamplePaths.SourcesPath, WorkspaceFolder)
                }).ToArray()
            };

            var json = JsonConvert.SerializeObject(launch, Formatting.Indented, new JsonSerializerSettings
            {
                ContractResolver = new CamelCasePropertyNamesContractResolver()
            });

            File.WriteAllText(ExamplePaths.LaunchJsonFile, json);
        }

        // ReSharper disable UnusedMember.Local
        // ReSharper disable UnusedAutoPropertyAccessor.Local
        /** launch.json */
        private class Launch
        {
            /** */
            public string Version { get; set; } = "0.2.0";

            /** */
            public LaunchConfig[] Configurations { get; set; }
        }

        /** launch.json configuration */
        private class LaunchConfig
        {
            /** */
            public string Name { get; set; }

            /** */
            public string Type { get; set; } = "coreclr";

            /** */
            public string Request { get; set; } = "launch";

            /** */
            public string PreLaunchTask { get; set; } = "build";

            /** */
            public string Program { get; set; } = string.Empty;

            /** */
            public string[] Args { get; set; } = Array.Empty<string>();

            /** */
            public string Cwd { get; set; } = "${workspaceFolder}/Thin/Cache/PutGet";

            /** */
            public string Console { get; set; } = "externalTerminal"; // Or "integratedTerminal"

            /** */
            public bool StopAtEntry { get; set; }
        }
    }
}
