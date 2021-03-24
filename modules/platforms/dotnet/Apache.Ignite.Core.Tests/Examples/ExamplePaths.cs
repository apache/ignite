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
    using System.Reflection;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Example paths.
    /// </summary>
    public static class ExamplePaths
    {
        /** */
        public const string SharedProjFileName = "Shared.csproj";

        /** */
        public static readonly string SourcesPath =
            Path.Combine(Impl.Common.IgniteHome.Resolve(), "modules", "platforms", "dotnet", "examples");

        /** */
        public static readonly string SlnFile = Path.Combine(SourcesPath, "Apache.Ignite.Examples.sln");

        /** */
        public static readonly string SharedProjFile = Path.Combine(SourcesPath, "Shared", SharedProjFileName);

        /** */
        public static readonly string LaunchJsonFile = Path.Combine(SourcesPath, ".vscode", "launch.json");

        /** */
        public static readonly string TasksJsonFile = Path.Combine(SourcesPath, ".vscode", "tasks.json");

        /** */
        public static readonly string ExpectedOutputDir = Path.Combine(
            Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
            "Examples",
            "ExpectedOutput");

        /// <summary>
        /// Gets the assembly file path.
        /// </summary>
        public static string GetAssemblyPath(string projFile)
        {
            var targetFw = GetTargetFramework(projFile);
            var name = Path.GetFileNameWithoutExtension(projFile);
            var path = Path.GetDirectoryName(projFile);

            return Path.Combine(path, "bin", "Debug", targetFw, $"{name}.dll");
        }

        /// <summary>
        /// Gets the target framework for the given project.
        /// </summary>
        public static string GetTargetFramework(string projFile)
        {
            return Regex.Match(File.ReadAllText(projFile), "<TargetFramework>(.*?)</TargetFramework>").Groups[1].Value;
        }
    }
}
