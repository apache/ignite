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

// ReSharper disable UnusedParameter.Global
namespace Apache.Ignite.Service
{
    using System;
    using Apache.Ignite.Core;

    /// <summary>
    /// .NET Core implementation of Ignite Service.
    /// </summary>
    public static class IgniteService
    {
        /// <summary>
        /// Uninstall the service.
        /// </summary>
        public static void Uninstall()
        {
            throw GetNotSupportedException();
        }

        /// <summary>
        /// Install the service.
        /// </summary>
        public static void DoInstall(Tuple<string, string>[] allArgs)
        {
            throw GetNotSupportedException();
        }

        /// <summary>
        /// Run the service.
        /// </summary>
        public static void Run(IgniteConfiguration cfg)
        {
            throw GetNotSupportedException();
        }

        /// <summary>
        /// Gets the exception.
        /// </summary>
        private static Exception GetNotSupportedException()
        {
            // Not supported right now, we should add cross-platform support when .NET Core 3 rolls out:
            // Windows Services and systemd on Linux).
            return new NotSupportedException("Ignite as Windows Service is not supported on .NET Core.");
        }
    }
}
