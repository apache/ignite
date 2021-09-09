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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using NUnit.Framework;

    /// <summary>
    /// Verifies source files.
    /// </summary>
    public class ProjectFilesTest
    {
        /// <summary>
        /// Tests that tools version is compatible with VS2010.
        /// </summary>
        [Test]
        public void TestCsprojToolsVersion()
        {
            var projFiles = TestUtils.GetDotNetSourceDir()
                .GetFiles("*.csproj", SearchOption.AllDirectories)
                .Where(x => !x.FullName.ToLower().Contains("dotnetcore") &&
                            !x.FullName.Contains("Benchmark") &&
                            !x.FullName.Contains("templates") &&
                            !x.FullName.Contains("examples"))
                .ToArray();

            Assert.GreaterOrEqual(projFiles.Length, 7);
            CheckFiles(projFiles, x => !x.Contains("ToolsVersion=\"4.0\""), "Invalid csproj files: ");
        }

        /// <summary>
        /// Tests that release build settings are correct: XML docs are generated.
        /// </summary>
        [Test]
        public void TestCsprojReleaseDocs()
        {
            CheckFiles(GetReleaseCsprojFiles(), x => !GetReleaseSection(x).Contains("DocumentationFile"),
                "Missing XML doc in release mode: ");
        }

        /// <summary>
        /// Tests that release build settings are correct: there are no DEBUG/TRACE constants.
        /// </summary>
        [Test]
        public void TestCsprojBuildSettings()
        {
            CheckFiles(GetReleaseCsprojFiles(), x => GetReleaseSection(x).Contains("DefineConstants"),
                "Invalid constants in release mode: ");
        }

        /// <summary>
        /// Tests that release build settings are correct: debug information is disabled.
        /// </summary>
        [Test]
        public void TestCsprojPdbSettings()
        {
            CheckFiles(GetReleaseCsprojFiles(), x => !GetReleaseSection(x).Contains("<DebugType>none</DebugType>"),
                "Invalid DebugType in release mode: ");
        }

        /// <summary>
        /// Tests that release build settings are correct: debug information is disabled.
        /// </summary>
        [Test]
        public void TestCsprojOptimizeCode()
        {
            CheckFiles(GetReleaseCsprojFiles(), x => !GetReleaseSection(x).Contains("<Optimize>true</Optimize>"),
                "Invalid optimize setting in release mode: ");
        }

        /// <summary>
        /// Tests that there are no public types in Apache.Ignite.Core.Impl namespace.
        /// </summary>
        [Test]
        public void TestImplNamespaceHasNoPublicTypes()
        {
            var excluded = new[]
            {
                "ProjectFilesTest.cs",
                "CopyOnWriteConcurrentDictionary.cs",
                "IgniteArgumentCheck.cs",
                "DelegateConverter.cs",
                "IgniteHome.cs",
                "TypeCaster.cs",
                "FutureType.cs",
                "CollectionExtensions.cs",
                "IQueryEntityInternal.cs",
                "ICacheInternal.cs",
                "CacheEntry.cs",
                "HandleRegistry.cs",
                "BinaryObjectHeader.cs"
            };

            var csFiles = TestUtils.GetDotNetSourceDir().GetFiles("*.cs", SearchOption.AllDirectories);

            foreach (var csFile in csFiles)
            {
                if (excluded.Contains(csFile.Name))
                {
                    continue;
                }

                var text = File.ReadAllText(csFile.FullName);

                if (!text.Contains("namespace Apache.Ignite.Core.Impl"))
                {
                    continue;
                }

                StringAssert.DoesNotContain("public class", text, csFile.FullName);
                StringAssert.DoesNotContain("public static class", text, csFile.FullName);
                StringAssert.DoesNotContain("public interface", text, csFile.FullName);
                StringAssert.DoesNotContain("public enum", text, csFile.FullName);
                StringAssert.DoesNotContain("public struct", text, csFile.FullName);
            }
        }

#if NETCOREAPP
        /// <summary>
        /// Tests that all .cs files are included in the project.
        /// </summary>
        [Test]
        public void TestAllCsharpFilesAreIncludedInProject()
        {
            var projFiles = TestUtils.GetDotNetSourceDir().GetFiles("*.csproj", SearchOption.AllDirectories)
                .Where(x =>
                    !x.Name.Contains("DotNetCore") &&
                    !x.Name.Contains("Benchmark") &&
                    !x.FullName.Contains("templates") &&
                    !x.FullName.Contains("examples"));

            var excludedFiles = new[]
            {
                "IgnitionStartTest.cs",
                "ShellTests.cs",
                "Common\\TestFixtureSetUp.cs",
                "Common\\TestFixtureTearDown.cs",
                "Client\\Cache\\CacheTestAsyncAwait.cs",
                "Cache\\CacheTestAsyncAwait.cs",
                "Compute\\ComputeTestAsyncAwait.cs"
            };

            Assert.Multiple(() =>
            {
                foreach (var projFile in projFiles)
                {
                    Assert.IsNotNull(projFile.Directory);

                    var projFileText = File.ReadAllText(projFile.FullName);
                    var csFiles = projFile.Directory.GetFiles("*.cs", SearchOption.AllDirectories);

                    foreach (var csFile in csFiles)
                    {
                        // Csproj uses the same path separator on all platforms.
                        var csFileRelativePath = Path.GetRelativePath(projFile.Directory.FullName, csFile.FullName)
                            .Replace(Path.DirectorySeparatorChar, '\\');

                        if (csFileRelativePath.StartsWith("bin\\") ||
                            csFileRelativePath.StartsWith("obj\\") ||
                            csFileRelativePath.Contains("DotNetCore") ||
                            csFileRelativePath.Contains("Examples") ||
                            excludedFiles.Contains(csFileRelativePath))
                        {
                            continue;
                        }

                        Assert.IsTrue(
                            projFileText.Contains(csFileRelativePath),
                            string.Format("Project file '{0}' should contain file '{1}'", projFile.Name, csFile));
                    }
                }
            });
        }
#endif

        /// <summary>
        /// Gets the csproj files that go to the release binary package.
        /// </summary>
        private static IEnumerable<FileInfo> GetReleaseCsprojFiles()
        {
            return TestUtils.GetDotNetSourceDir().GetFiles("*.csproj", SearchOption.AllDirectories)
                .Where(x => x.Name != "Apache.Ignite.csproj" &&
                            !x.Name.Contains("Test") &&
                            !x.FullName.Contains("examples") &&
                            !x.FullName.Contains("templates") &&
                            !x.Name.Contains("DotNetCore") &&
                            !x.Name.Contains("Benchmark"));
        }

        /// <summary>
        /// Gets the release section.
        /// </summary>
        private static string GetReleaseSection(string csproj)
        {
            return Regex.Match(csproj, @"<PropertyGroup[^>]*Release\|AnyCPU(.*?)<\/PropertyGroup>",
                RegexOptions.Singleline).Value;
        }

        /// <summary>
        /// Tests that tools version is compatible with VS2010.
        /// </summary>
        [Test]
        public void TestSlnToolsVersion()
        {
            var slnFiles = TestUtils.GetDotNetSourceDir()
                .GetFiles("*.sln", SearchOption.AllDirectories)
                .Where(x => !x.Name.Contains("DotNetCore") && !x.Name.Contains("Examples"))
                .ToArray();

            Assert.GreaterOrEqual(slnFiles.Length, 2);
            CheckFiles(slnFiles, x => !x.Contains("# Visual Studio 2010") ||
                                      !x.Contains("Microsoft Visual Studio Solution File, Format Version 11.00"),
                "Invalid sln files: ");
        }

        /// <summary>
        /// Tests that there are no non-ASCII chars.
        /// </summary>
        [Test]
        public void TestAsciiChars()
        {
            var allowedFiles = new[]
            {
                "BinaryStringTest.cs",
                "BinarySelfTest.cs",
                "CacheDmlQueriesTest.cs",
                "CacheTest.cs",
                "PartitionAwarenessTest.cs"
            };

            var srcFiles = TestUtils.GetDotNetSourceDir()
                .GetFiles("*.cs", SearchOption.AllDirectories)
                .Where(x => !allowedFiles.Contains(x.Name));

            CheckFiles(srcFiles, x => x.Any(ch => ch > 255), "Files with non-ASCII chars: ");
        }

        /// <summary>
        /// Checks the files.
        /// </summary>
        private static void CheckFiles(IEnumerable<FileInfo> files, Func<string, bool> isInvalid, string errorText)
        {
            var invalidFiles = files.Where(x => isInvalid(File.ReadAllText(x.FullName))).ToArray();

            Assert.AreEqual(0, invalidFiles.Length,
                errorText + string.Join("\n ", invalidFiles.Select(x => x.FullName)));
        }
    }
}
