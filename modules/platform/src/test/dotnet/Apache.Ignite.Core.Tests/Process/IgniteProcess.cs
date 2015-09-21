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

namespace Apache.Ignite.Core.Tests.Process
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Apache.Ignite.Core.Impl;

    /// <summary>
    /// Defines forked Ignite node.
    /// </summary>
    public class IgniteProcess
    {
        /** Executable file name. */
        private static readonly string ExeName = "Apache.Ignite.exe";

        /** Executable process name. */
        private static readonly string ExeProcName = ExeName.Substring(0, ExeName.LastIndexOf('.'));

        /** Executable configuration file name. */
        private static readonly string ExeCfgName = ExeName + ".config";

        /** Executable backup configuration file name. */
        private static readonly string ExeCfgBakName = ExeCfgName + ".bak";

        /** Directory where binaries are stored. */
        private static readonly string ExeDir;

        /** Full path to executable. */
        private static readonly string ExePath;

        /** Full path to executable configuration file. */
        private static readonly string ExeCfgPath;

        /** Full path to executable configuration file backup. */
        private static readonly string ExeCfgBakPath;

        /** Default process output reader. */
        private static readonly IIgniteProcessOutputReader DfltOutReader = new IgniteProcessConsoleOutputReader();

        /** Process. */
        private readonly Process _proc;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static IgniteProcess()
        {
            // 1. Locate executable file and related stuff.
            DirectoryInfo dir = new FileInfo(new Uri(typeof(IgniteProcess).Assembly.CodeBase).LocalPath).Directory;

            // ReSharper disable once PossibleNullReferenceException
            ExeDir = dir.FullName;

            var exe = dir.GetFiles(ExeName);

            if (exe.Length == 0)
                throw new Exception(ExeName + " is not found in test output directory: " + dir.FullName);

            ExePath = exe[0].FullName;

            var exeCfg = dir.GetFiles(ExeCfgName);

            if (exeCfg.Length == 0)
                throw new Exception(ExeCfgName + " is not found in test output directory: " + dir.FullName);

            ExeCfgPath = exeCfg[0].FullName;

            ExeCfgBakPath = Path.Combine(ExeDir, ExeCfgBakName);

            File.Delete(ExeCfgBakPath);
        }

        /// <summary>
        /// Save current configuration to backup.
        /// </summary>
        public static void SaveConfigurationBackup()
        {
            File.Copy(ExeCfgPath, ExeCfgBakPath, true);
        }

        /// <summary>
        /// Restore configuration from backup.
        /// </summary>
        public static void RestoreConfigurationBackup()
        {
            File.Copy(ExeCfgBakPath, ExeCfgPath, true);
        }

        /// <summary>
        /// Replace application configuration with another one.
        /// </summary>
        /// <param name="relPath">Path to config relative to executable directory.</param>
        public static void ReplaceConfiguration(string relPath)
        {
            File.Copy(Path.Combine(ExeDir, relPath), ExeCfgPath, true);
        }

        /// <summary>
        /// Kill all Ignite processes.
        /// </summary>
        public static void KillAll()
        {
            foreach (Process proc in Process.GetProcesses())
            {
                if (proc.ProcessName.Equals(ExeProcName))
                {
                    proc.Kill();

                    proc.WaitForExit();
                }
            }
        }

        /// <summary>
        /// Construector.
        /// </summary>
        /// <param name="args">Arguments</param>
        public IgniteProcess(params string[] args) : this(DfltOutReader, args) { }

        /// <summary>
        /// Construector.
        /// </summary>
        /// <param name="outReader">Output reader.</param>
        /// <param name="args">Arguments.</param>
        public IgniteProcess(IIgniteProcessOutputReader outReader, params string[] args)
        {
            // Add test dll path
            args = args.Concat(new[] {"-assembly=" + GetType().Assembly.Location}).ToArray();

            _proc = Start(ExePath, IgniteManager.GetIgniteHome(null), outReader, args);
        }

        /// <summary>
        /// Starts a grid process.
        /// </summary>
        /// <param name="exePath">Exe path.</param>
        /// <param name="ggHome">Ignite home.</param>
        /// <param name="outReader">Output reader.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>Started process.</returns>
        public static Process Start(string exePath, string ggHome, IIgniteProcessOutputReader outReader = null, 
            params string[] args)
        {
            Debug.Assert(!string.IsNullOrEmpty(exePath));
            Debug.Assert(!string.IsNullOrEmpty(ggHome));

            // 1. Define process start configuration.
            var sb = new StringBuilder();

            foreach (string arg in args)
                sb.Append('\"').Append(arg).Append("\" ");

            var procStart = new ProcessStartInfo
            {
                FileName = exePath,
                Arguments = sb.ToString()
            };

            if (!string.IsNullOrEmpty(ggHome))
                procStart.EnvironmentVariables[IgniteManager.EnvIgniteHome] = ggHome;

            procStart.EnvironmentVariables[IgniteManager.EnvIgniteNativeTestClasspath] = "true";

            procStart.CreateNoWindow = true;
            procStart.UseShellExecute = false;

            procStart.RedirectStandardOutput = true;
            procStart.RedirectStandardError = true;

            var workDir = Path.GetDirectoryName(exePath);

            if (workDir != null)
                procStart.WorkingDirectory = workDir;

            Console.WriteLine("About to run Apache.Ignite.exe process [exePath=" + exePath + ", arguments=" + sb + ']');

            // 2. Start.
            var proc = Process.Start(procStart);

            Debug.Assert(proc != null);

            // 3. Attach output readers to avoid hangs.
            outReader = outReader ?? DfltOutReader;

            Attach(proc, proc.StandardOutput, outReader, false);
            Attach(proc, proc.StandardError, outReader, true);

            return proc;
        }

        /// <summary>
        /// Whether the process is still alive.
        /// </summary>
        public bool Alive
        {
            get { return !_proc.HasExited; }
        }

        /// <summary>
        /// Kill process.
        /// </summary>
        public void Kill()
        {
            _proc.Kill();
        }

        /// <summary>
        /// Join process.
        /// </summary>
        /// <returns>Exit code.</returns>
        public int Join()
        {
            _proc.WaitForExit();

            return _proc.ExitCode;
        }

        /// <summary>
        /// Join process with timeout.
        /// </summary>
        /// <param name="timeout">Timeout in milliseconds.</param>
        /// <returns><c>True</c> if process exit occurred before timeout.</returns>
        public bool Join(int timeout)
        {
            return _proc.WaitForExit(timeout);
        }

        /// <summary>
        /// Join process with timeout.
        /// </summary>
        /// <param name="timeout">Timeout in milliseconds.</param>
        /// <param name="exitCode">Exit code.</param>
        /// <returns><c>True</c> if process exit occurred before timeout.</returns>
        public bool Join(int timeout, out int exitCode)
        {
            if (_proc.WaitForExit(timeout))
            {
                exitCode = _proc.ExitCode;

                return true;
            }
            exitCode = 0;

            return false;
        }

        /// <summary>
        /// Attach output reader to the process.
        /// </summary>
        /// <param name="proc">Process.</param>
        /// <param name="reader">Process stream reader.</param>
        /// <param name="outReader">Output reader.</param>
        /// <param name="err">Whether this is error stream.</param>
        private static void Attach(Process proc, StreamReader reader, IIgniteProcessOutputReader outReader, bool err)
        {
            new Thread(() =>
            {
                while (!proc.HasExited)
                    outReader.OnOutput(proc, reader.ReadLine(), err);
            }) {IsBackground = true}.Start();
        }
    }
}
