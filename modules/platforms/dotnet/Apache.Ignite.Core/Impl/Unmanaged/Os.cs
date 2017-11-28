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

namespace Apache.Ignite.Core.Impl.Unmanaged
{
    using System;

    /// <summary>
    /// Operating system / platform detector.
    /// </summary>
    internal static class Os
    {
        /// <summary>
        /// Initializes the <see cref="Os"/> class.
        /// </summary>
        static Os()
        {
            var platform = Environment.OSVersion.Platform;

            IsLinux = platform == PlatformID.Unix
                      || platform == PlatformID.MacOSX
                      || (int) Environment.OSVersion.Platform == 128;

            IsWindows = platform == PlatformID.Win32NT
                        || platform == PlatformID.Win32S
                        || platform == PlatformID.Win32Windows;

            IsMacOs = IsLinux && Shell.BashExecute("uname").Contains("Darwin");
            IsMono = Type.GetType("Mono.Runtime") != null;
            IsNetCore = !IsMono;
        }

        /// <summary>
        /// .NET Core.
        /// </summary>
        public static bool IsNetCore { get; set; }

        /// <summary>
        /// Mono.
        /// </summary>
        public static bool IsMono { get; private set; }

        /// <summary>
        /// Windows.
        /// </summary>
        public static bool IsWindows { get; private set; }

        /// <summary>
        /// Linux.
        /// </summary>
        public static bool IsLinux { get; private set; }

        /// <summary>
        /// MacOs.
        /// </summary>
        public static bool IsMacOs { get; private set; }
    }
}
