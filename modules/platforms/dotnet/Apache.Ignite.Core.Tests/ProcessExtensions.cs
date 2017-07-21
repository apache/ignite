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
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Process extensions.
    /// </summary>
    public static class ProcessExtensions
    {
        /** */
        private const int ThreadAccessSuspendResume = 0x2;

        /** */
        [DllImport("kernel32.dll")]
        private static extern IntPtr OpenThread(int dwDesiredAccess, bool bInheritHandle, uint dwThreadId);

        /** */
        [DllImport("kernel32.dll")]
        private static extern uint SuspendThread(IntPtr hThread);

        /** */
        [DllImport("kernel32.dll")]
        private static extern int ResumeThread(IntPtr hThread);

        /// <summary>
        /// Suspends the specified process.
        /// </summary>
        /// <param name="process">The process.</param>
        public static void Suspend(this System.Diagnostics.Process process)
        {
            foreach (var thread in process.Threads.Cast<ProcessThread>())
            {
                var pOpenThread = OpenThread(ThreadAccessSuspendResume, false, (uint)thread.Id);

                if (pOpenThread == IntPtr.Zero)
                    break;

                SuspendThread(pOpenThread);
            }
        }
        /// <summary>
        /// Resumes the specified process.
        /// </summary>
        /// <param name="process">The process.</param>
        public static void Resume(this System.Diagnostics.Process process)
        {
            foreach (var thread in process.Threads.Cast<ProcessThread>())
            {
                var pOpenThread = OpenThread(ThreadAccessSuspendResume, false, (uint)thread.Id);

                if (pOpenThread == IntPtr.Zero)
                    break;

                ResumeThread(pOpenThread);
            }
        }
    }
}