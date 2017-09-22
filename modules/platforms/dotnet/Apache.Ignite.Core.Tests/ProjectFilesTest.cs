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
    using System.Reflection;
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
            var projFiles = GetDotNetSourceDir().GetFiles("*.csproj", SearchOption.AllDirectories);
            Assert.GreaterOrEqual(projFiles.Length, 7);

            CheckFiles(projFiles, x => !x.Contains("ToolsVersion=\"4.0\""), "Invalid csproj files: ");
        }

        /// <summary>
        /// Tests that tools version is compatible with VS2010.
        /// </summary>
        [Test]
        public void TestSlnToolsVersion()
        {
            var slnFiles = GetDotNetSourceDir().GetFiles("*.sln", SearchOption.AllDirectories);
            Assert.GreaterOrEqual(slnFiles.Length, 2);

            CheckFiles(slnFiles, x => !x.Contains("# Visual Studio 2010") ||
                                      !x.Contains("Microsoft Visual Studio Solution File, Format Version 11.00"),
                "Invalid sln files: ");
        }

        /// <summary>
        /// Tests that there are no Cyrillic C instead of English C (which are on the same keyboard key).
        /// </summary>
        [Test]
        public void TestCyrillicChars()
        {
            var srcFiles = GetDotNetSourceDir().GetFiles("*.cs", SearchOption.AllDirectories);

            CheckFiles(srcFiles, x => x.Contains('\u0441') || x.Contains('\u0421'), "Files with Cyrillic 'C': ");
        }

        /// <summary>
        /// Checks the files.
        /// </summary>
        private static void CheckFiles(IEnumerable<FileInfo> files, Func<string, bool> isInvalid, string errorText)
        {
            var invalidFiles = files.Where(x => isInvalid(File.ReadAllText(x.FullName))).ToArray();

            Assert.AreEqual(0, invalidFiles.Length,
                errorText + string.Join(", ", invalidFiles.Select(x => x.FullName)));
        }

        /// <summary>
        /// Gets the dot net source dir.
        /// </summary>
        private static DirectoryInfo GetDotNetSourceDir()
        {
            // ReSharper disable once AssignNullToNotNullAttribute
            var dir = new DirectoryInfo(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location));

            while (dir != null)
            {
                if (dir.GetFiles().Any(x => x.Name == "Apache.Ignite.sln"))
                    return dir;

                dir = dir.Parent;
            }

            throw new InvalidOperationException("Could not resolve Ignite.NET source directory.");
        }
    }
}
