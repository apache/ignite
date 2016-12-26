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