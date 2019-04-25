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

    /// <summary>
    /// Grid path resolver.
    /// </summary>
    public static class PathUtil
    {
        /** */
        public const string DevPrefix = "modules\\";

        /** */
        public static readonly string IgniteHome = Core.Impl.Common.IgniteHome.Resolve(null);

        /// <summary>
        /// Examples source code path.
        /// </summary>
        public static readonly string ExamplesSourcePath =
            Path.Combine(IgniteHome, @"modules\platforms\dotnet\examples");

        /// <summary>
        /// Examples application configuration path.
        /// </summary>
        public static readonly string ExamplesAppConfigPath =
            Path.Combine(ExamplesSourcePath, @"Apache.Ignite.Examples\App.config");

        /// <summary>
        /// Gets the full configuration path.
        /// </summary>
        public static string GetFullConfigPath(string springConfigUrl)
        {
            if (string.IsNullOrEmpty(springConfigUrl))
                return springConfigUrl;

            return Path.GetFullPath(Path.Combine(IgniteHome, DevPrefix + springConfigUrl));
        }
    }
}