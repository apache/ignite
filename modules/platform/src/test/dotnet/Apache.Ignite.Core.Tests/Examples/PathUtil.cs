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

    /// <summary>
    /// Grid path resolver.
    /// </summary>
    public static class PathUtil
    {
        /** Relative GG homa path. */
        private const string HomePath = @"..\..\..\..\..\..\..\..\..";

        /** Relative exe path. */
        private const string ExePath = HomePath + @"\modules\clients\dotnet\bin\" + ExeName + ".exe";

        /** Current bin path. */
        private static readonly string BinPath = Path.GetDirectoryName(typeof(PathUtil).Assembly.Location);

        /// <summary>
        /// GridGain executable name.
        /// </summary>
        public const string ExeName = "GridGain";

        /// <summary>
        /// Full GridGain.exe path.
        /// </summary>
        public static readonly string GridGainExePath = Path.GetFullPath(Path.Combine(BinPath, ExePath));

        /// <summary>
        /// GridGain home.
        /// </summary>
        public static readonly string GridGainHome = Path.GetFullPath(Path.Combine(BinPath, HomePath));

        /// <summary>
        /// Examples source code path.
        /// </summary>
        public static readonly string ExamplesSourcePath = Path.Combine(GridGainHome,
            @"examples\clients\dotnet\GridGainExamples");

        /// <summary>
        /// Gets the full configuration path.
        /// </summary>
        public static string GetFullConfigPath(string springConfigUrl)
        {
            if (string.IsNullOrEmpty(springConfigUrl))
                return springConfigUrl;

            return Path.GetFullPath(Path.Combine(GridGainHome, springConfigUrl));
        }
    }
}