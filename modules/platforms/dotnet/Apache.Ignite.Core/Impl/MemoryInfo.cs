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

namespace Apache.Ignite.Core.Impl
{
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Memory info.
    /// </summary>
    internal static class MemoryInfo
    {
        /// <summary>
        /// Gets the total physical memory.
        /// </summary>
        public static ulong GetTotalPhysicalMemory(ulong defaultValue)
        {
            if (Os.IsWindows)
            {
                return NativeMethodsWindows.GlobalMemoryStatusExTotalPhys();
            }

            const string memInfo = "/proc/meminfo";

            if (File.Exists(memInfo))
            {
                var kbytes = File.ReadAllLines(memInfo).Select(x => Regex.Match(x, @"MemTotal:\s+([0-9]+) kB"))
                    .Where(x => x.Success)
                    .Select(x => x.Groups[1].Value).FirstOrDefault();

                if (kbytes != null)
                {
                    return ulong.Parse(kbytes) * 1024;
                }
            }

            return defaultValue;
        }

        /// <summary>
        /// Native methods.
        /// </summary>
        private static class NativeMethodsWindows
        {
            /// <summary>
            /// Gets the total physical memory.
            /// </summary>
            internal static ulong GlobalMemoryStatusExTotalPhys()
            {
                var status = new MEMORYSTATUSEX();
                status.Init();

                GlobalMemoryStatusEx(ref status);

                return status.ullTotalPhys;
            }

            /// <summary>
            /// Globals the memory status.
            /// </summary>
            [return: MarshalAs(UnmanagedType.Bool)]
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
            [SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
            private static extern bool GlobalMemoryStatusEx([In, Out] ref MEMORYSTATUSEX lpBuffer);

            [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
            // ReSharper disable InconsistentNaming
            // ReSharper disable MemberCanBePrivate.Local
            private struct MEMORYSTATUSEX
            {
                public uint dwLength;
                public readonly uint dwMemoryLoad;
                public readonly ulong ullTotalPhys;
                public readonly ulong ullAvailPhys;
                public readonly ulong ullTotalPageFile;
                public readonly ulong ullAvailPageFile;
                public readonly ulong ullTotalVirtual;
                public readonly ulong ullAvailVirtual;
                public readonly ulong ullAvailExtendedVirtual;

                /// <summary>
                /// Initializes a new instance of the <see cref="MEMORYSTATUSEX"/> struct.
                /// </summary>
                public void Init()
                {
                    dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
                }
            }
        }
    }
}