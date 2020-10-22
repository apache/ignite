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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Reads cgroup limits for the current process.
    /// <para />
    /// Based on cgroup handling in CLR:
    /// https://github.com/dotnet/runtime/blob/master/src/coreclr/src/gc/unix/cgroup.cpp
    /// </summary>
    internal static class CGroup
    {
        /// <summary>
        /// Gets cgroup memory limit in bytes.
        /// </summary>
        public static readonly ulong? MemoryLimitInBytes = GetMemoryLimitInBytes();

        /** */
        private const string MemorySubsystem = "memory";

        /** */
        private const string MemoryLimitFileName = "memory.limit_in_bytes";

        /** */
        private const string ProcMountInfoFileName = "/proc/self/mountinfo";

        /** */
        private const string ProcCGroupFileName = "/proc/self/cgroup";

        /// <summary>
        /// Gets memory limit in bytes.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static ulong? GetMemoryLimitInBytes()
        {
            if (Os.IsWindows)
            {
                return null;
            }

            try
            {
                var memMount = FindHierarchyMount(MemorySubsystem);
                if (memMount == null)
                {
                    return null;
                }

                var cgroupPathRelativeToMount = FindCGroupPath(MemorySubsystem);
                if (cgroupPathRelativeToMount == null)
                {
                    return null;
                }

                var hierarchyMount = memMount.Value.Key;
                var hierarchyRoot = memMount.Value.Value;

                // Host CGroup: append the relative path
                // In Docker: root and relative path are the same
                var groupPath =
                    string.Equals(hierarchyRoot, cgroupPathRelativeToMount, StringComparison.Ordinal)
                        ? hierarchyMount
                        : hierarchyMount + cgroupPathRelativeToMount;

                var memLimitFilePath = Path.Combine(groupPath, MemoryLimitFileName);

                var memLimitText = File.ReadAllText(memLimitFilePath);

                ulong memLimit;
                if (ulong.TryParse(memLimitText, out memLimit))
                {
                    return memLimit;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Failed to determine cgroup memory limit: " + e);
            }

            return null;
        }

        /// <summary>
        /// Finds the hierarchy mount and root for the current process.
        /// </summary>
        private static KeyValuePair<string, string>? FindHierarchyMount(string subsystem)
        {
            foreach (var line in File.ReadAllLines(ProcMountInfoFileName))
            {
                var mount = GetHierarchyMount(line, subsystem);

                if (mount != null)
                {
                    return mount;
                }
            }

            return null;
        }

        /// <summary>
        /// Get the hierarchy mount and root.
        /// </summary>
        private static KeyValuePair<string, string>? GetHierarchyMount(string mountInfo, string subsystem)
        {
            // Example: 41 34 0:35 / /sys/fs/cgroup/memory rw,nosuid,nodev shared:17 - cgroup cgroup rw,memory
            const string cGroup = " - cgroup ";

            var cgroupIdx = mountInfo.IndexOf(cGroup, StringComparison.Ordinal);

            if (cgroupIdx < 0)
                return null;

            var optionsIdx = mountInfo.LastIndexOf(" ", cgroupIdx + cGroup.Length, StringComparison.Ordinal);

            if (optionsIdx < 0)
                return null;

            var memIdx = mountInfo.IndexOf(subsystem, optionsIdx + 1, StringComparison.Ordinal);

            if (memIdx < 0)
                return null;

            var parts = mountInfo.Split(' ');

            if (parts.Length < 5)
                return null;

            return new KeyValuePair<string, string>(parts[4], parts[3]);
        }

        /// <summary>
        /// Finds the cgroup path for the current process.
        /// </summary>
        private static string FindCGroupPath(string subsystem)
        {
            var lines = File.ReadAllLines(ProcCGroupFileName);

            foreach (var line in lines)
            {
                var parts = line.Split(new[] {':'}, 3);

                if (parts.Length == 3 && parts[1].Split(',').Contains(subsystem, StringComparer.Ordinal))
                {
                    return parts[2];
                }
            }

            return null;
        }
    }
}
