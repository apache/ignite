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
    using System.IO;

    /// <summary>
    /// Path utils.
    /// </summary>
    public static class PathUtils
    {
        /// <summary>
        /// Creates a uniquely named, empty temporary directory on disk and returns the full path of that directory.
        /// </summary>
        /// <returns>The full path of the temporary directory.</returns>
        public static string GetTempDirectoryName()
        {
            var baseDir = Path.Combine(Path.GetTempPath(), "ignite_test_");

            while (true)
            {
                try
                {
                    return Directory.CreateDirectory(baseDir + Path.GetRandomFileName()).FullName;
                }
                catch (IOException)
                {
                    // Expected
                }
                catch (UnauthorizedAccessException)
                {
                    // Expected
                }
            }
        }

        /// <summary>
        /// Copies directory recursively.
        /// </summary>
        /// <param name="source">Source path.</param>
        /// <param name="target">Target path.</param>
        public static void CopyDirectory(string source, string target)
        {
            foreach (var dir in Directory.GetDirectories(source, "*", SearchOption.AllDirectories))
                Directory.CreateDirectory(dir.Replace(source, target));

            foreach (var file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
                File.Copy(file, file.Replace(source, target), true);
        }
    }
}
