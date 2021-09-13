/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using NUnit.Framework;

    /// <summary>
    /// Checks project files.
    /// </summary>
    public class ProjectFilesTests
    {
        private static readonly IReadOnlyCollection<string> Types = new[]
        {
            "class", "struct", "record", "enum", "interface"
        };

        private static readonly string InternalDir =
            $"{Path.DirectorySeparatorChar}Internal{Path.DirectorySeparatorChar}";

        [Test]
        public void TestInternalNamespaceHasNoPublicTypes()
        {
            foreach (var file in GetCsFiles())
            {
                if (!file.Contains(InternalDir, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var text = File.ReadAllText(file);

                foreach (var type in Types)
                {
                    StringAssert.DoesNotContain("public " + type, text, file);
                }
            }
        }

        [Test]
        public void TestPublicTypesAreSealed()
        {
            foreach (var file in GetCsFiles())
            {
                if (file.Contains(InternalDir, StringComparison.Ordinal) ||
                    file.Contains(".Tests", StringComparison.Ordinal) ||
                    file.Contains(".Benchmarks", StringComparison.Ordinal) ||
                    file.EndsWith("Exception.cs", StringComparison.Ordinal))
                {
                    continue;
                }

                var text = File.ReadAllText(file);

                if (text.Contains("public class", StringComparison.Ordinal) ||
                    text.Contains("public record", StringComparison.Ordinal))
                {
                    Assert.Fail("Public classes must be sealed: " + file);
                }
            }
        }

        [Test]
        public void TestTodosHaveTickets()
        {
            Assert.Multiple(() =>
            {
                foreach (var file in GetCsFiles())
                {
                    if (file.EndsWith("ProjectFilesTests.cs", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    foreach (var line in File.ReadAllLines(file))
                    {
                        if (line.Contains("TODO", StringComparison.Ordinal))
                        {
                            StringAssert.Contains("IGNITE-", line, "TODOs should be linked to tickets: " + file);
                        }
                    }
                }
            });
        }

        private static IEnumerable<string> GetCsFiles()
        {
            return Directory.GetFiles(TestUtils.SolutionDir, "*.cs", SearchOption.AllDirectories);
        }
    }
}
