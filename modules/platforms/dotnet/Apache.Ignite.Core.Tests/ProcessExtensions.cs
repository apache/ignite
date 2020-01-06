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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Tests.Process;

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
        
        /// <summary>
        /// Attaches the process console reader.
        /// </summary>
        public static void AttachProcessConsoleReader(this System.Diagnostics.Process proc, 
            params IIgniteProcessOutputReader[] outReaders)
        {
            var outReader = outReaders == null || outReaders.Length == 0
                ? (IIgniteProcessOutputReader) new IgniteProcessConsoleOutputReader()
                : new IgniteProcessCompositeOutputReader(outReaders);

            Attach(proc, proc.StandardOutput, outReader, false);
            Attach(proc, proc.StandardError, outReader, true);
        }
        
        
        /// <summary>
        /// Attach output reader to the process.
        /// </summary>
        /// <param name="proc">Process.</param>
        /// <param name="reader">Process stream reader.</param>
        /// <param name="outReader">Output reader.</param>
        /// <param name="err">Whether this is error stream.</param>
        private static void Attach(System.Diagnostics.Process proc, StreamReader reader, 
            IIgniteProcessOutputReader outReader, bool err)
        {
            new Thread(() =>
            {
                while (!proc.HasExited)
                    outReader.OnOutput(proc, reader.ReadLine(), err);
            }) {IsBackground = true}.Start();
        }
               
        /// <summary>
        /// Kills process tree.
        /// </summary>
        public static void KillProcessTree(this System.Diagnostics.Process process)
        {
            process.EnableRaisingEvents = true;
            
            if (Os.IsWindows)
            {
                Execute("taskkill", string.Format("/T /F /PID {0}", process.Id));
                process.WaitForExit();
            }
            else
            {
                var children = new HashSet<int>();
                GetProcessChildIdsUnix(process.Id, children);
                
                foreach (var childId in children)
                {
                    KillProcessUnix(childId);
                }
                
                KillProcessUnix(process.Id);
            }
            
            // NOTE: This can hang on Linux if being ran after anything that uses Ignite Persistence.
            // Process becomes a zombie for some reason (Java meddling with SIGCHLD?)
            if (!process.WaitForExit(10000))
            {
                throw new Exception("Failed to kill process: " + process.Id);
            }
        }

        /// <summary>
        /// Runs a process and waits for exit.
        /// </summary>
        private static void Execute(string file, string args, params IIgniteProcessOutputReader[] readers)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = file,
                Arguments = args,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };

            var process = new System.Diagnostics.Process {StartInfo = startInfo};
            process.Start();

            process.AttachProcessConsoleReader(readers);
            if (!process.WaitForExit(1000))
            {
                process.Kill();
            }
        }

        /// <summary>
        /// Gets process child ids.
        /// </summary>
        private static void GetProcessChildIdsUnix(int parentId, ISet<int> children)
        {
            var reader = new ListDataReader();
            Execute("pgrep", string.Format("-P {0}", parentId), reader);

            foreach (var line in reader.GetOutput())
            {
                int id;
                if (int.TryParse(line, out id))
                {
                    children.Add(id);
                    GetProcessChildIdsUnix(id, children);
                }
            }
        }

        /// <summary>
        /// Kills Unix process.
        /// </summary>
        private static void KillProcessUnix(int processId)
        {
            Execute("kill", string.Format("-KILL {0}", processId));
        }
    }
}