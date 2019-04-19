/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    using System.Linq;
    using System.Text.RegularExpressions;
    using NUnit.Framework;

    /// <summary>
    /// Tests project files.
    /// </summary>
    public class ProjectFilesTest
    {
        /// <summary>
        /// Checks config files in examples comments for existence.
        /// </summary>
        [Test]
        public void CheckConfigFilesExist()
        {
            var paths = Directory.GetFiles(PathUtil.ExamplesSourcePath, "*.cs", SearchOption.AllDirectories)
                .Select(File.ReadAllText)
                .SelectMany(src => Regex.Matches(src, @"platforms[^\s]+.config").OfType<Match>())
                .Where(match => match.Success)
                .Select(match => PathUtil.GetFullConfigPath(match.Value))
                .Distinct()
                .ToList();

            Assert.AreEqual(1, paths.Count);

            paths.ForEach(path => Assert.IsTrue(File.Exists(path), "Config file does not exist: " + path));
        }
    }
}