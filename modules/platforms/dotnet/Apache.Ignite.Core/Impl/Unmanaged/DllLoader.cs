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
    using System.Runtime.InteropServices;

    /// <summary>
    /// Dynamically loads unmanaged DLLs with respect to current platform.
    /// </summary>
    internal class DllLoader
    {
        /** Lazy symbol binding. */
        private const int RtldLazy = 1;

        /** Global symbol access. */
        private const int RtldGlobal = 8;

        /// <summary>
        /// Loads specified DLL.
        /// </summary>
        public static IntPtr Load(string dllPath)
        {
            // TODO: Handle errors in a platform-specific way.
            if (Os.IsWindows)
            {
                return Windows.LoadLibrary(dllPath);
            }

            if (Os.IsLinux)
            {
                if (Os.IsMono)
                {
                    return Mono.dlopen(dllPath, RtldGlobal | RtldLazy);
                }

                if (Os.IsNetCore)
                {
                    return Core.dlopen(dllPath, RtldGlobal | RtldLazy);
                }

                return Linux.dlopen(dllPath, RtldGlobal | RtldLazy);
            }

            throw new InvalidOperationException("Unsupported OS: " + Environment.OSVersion);
        }

        /// <summary>
        /// Windows.
        /// </summary>
        private static class Windows
        {
            [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr LoadLibrary(string filename);
        }

        /// <summary>
        /// Linux.
        /// </summary>
        private static class Linux
        {
            [DllImport("libdl.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlopen(string filename, int flags);
        }

        /// <summary>
        /// libdl.so depends on libc6-dev on Linux, use Mono instead.
        /// </summary>
        private static class Mono
        {
            [DllImport("__Internal", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlopen(string filename, int flags);
        }

        /// <summary>
        /// libdl.so depends on libc6-dev on Linux, use libcoreclr instead.
        /// </summary>
        private static class Core
        {
            [DllImport("libcoreclr.so", SetLastError = true, CharSet = CharSet.Ansi, BestFitMapping = false,
                ThrowOnUnmappableChar = true)]
            internal static extern IntPtr dlopen(string filename, int flags);
        }
    }
}