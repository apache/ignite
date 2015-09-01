/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
    /// Defines forked GridGain node.
    /// </summary>
    public class GridProcess
    {
        /** Executable file name. */
        private static readonly string EXE_NAME = "GridGain.exe";

        /** Executable process name. */
        private static readonly string EXE_PROC_NAME = EXE_NAME.Substring(0, EXE_NAME.IndexOf('.'));

        /** Executable configuration file name. */
        private static readonly string EXE_CFG_NAME = EXE_NAME + ".config";

        /** Executable backup configuration file name. */
        private static readonly string EXE_CFG_BAK_NAME = EXE_CFG_NAME + ".bak";

        /** Directory where binaries are stored. */
        private static readonly string EXE_DIR;

        /** Full path to executable. */
        private static readonly string EXE_PATH;

        /** Full path to executable configuration file. */
        private static readonly string EXE_CFG_PATH;

        /** Full path to executable configuration file backup. */
        private static readonly string EXE_CFG_BAK_PATH;

        /** Default process output reader. */
        private static readonly IGridProcessOutputReader DFLT_OUT_READER = new GridProcessConsoleOutputReader();

        /** Process. */
        private readonly Process proc;

        /// <summary>
        /// Static initializer.
        /// </summary>
        static GridProcess()
        {
            // 1. Locate executable file and related stuff.
            DirectoryInfo dir = new FileInfo(new Uri(typeof(GridProcess).Assembly.CodeBase).LocalPath).Directory;

            // ReSharper disable once PossibleNullReferenceException
            EXE_DIR = dir.FullName;

            FileInfo[] exe = dir.GetFiles(EXE_NAME);

            if (exe.Length == 0)
                throw new Exception(EXE_NAME + " is not found in test output directory: " + dir.FullName);

            EXE_PATH = exe[0].FullName;

            FileInfo[] exeCfg = dir.GetFiles(EXE_CFG_NAME);

            if (exeCfg.Length == 0)
                throw new Exception(EXE_CFG_NAME + " is not found in test output directory: " + dir.FullName);

            EXE_CFG_PATH = exeCfg[0].FullName;

            EXE_CFG_BAK_PATH = Path.Combine(EXE_DIR, EXE_CFG_BAK_NAME);

            File.Delete(EXE_CFG_BAK_PATH);
        }

        /// <summary>
        /// Save current configuration to backup.
        /// </summary>
        public static void SaveConfigurationBackup()
        {
            File.Copy(EXE_CFG_PATH, EXE_CFG_BAK_PATH, true);
        }

        /// <summary>
        /// Restore configuration from backup.
        /// </summary>
        public static void RestoreConfigurationBackup()
        {
            File.Copy(EXE_CFG_BAK_PATH, EXE_CFG_PATH, true);
        }

        /// <summary>
        /// Replace application configuration with another one.
        /// </summary>
        /// <param name="relPath">Path to config relative to executable directory.</param>
        public static void ReplaceConfiguration(string relPath)
        {
            File.Copy(Path.Combine(EXE_DIR, relPath), EXE_CFG_PATH, true);
        }

        /// <summary>
        /// Kill all GridGain processes.
        /// </summary>
        public static void KillAll()
        {
            foreach (Process proc in Process.GetProcesses())
            {
                if (proc.ProcessName.Equals(EXE_PROC_NAME))
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
        public GridProcess(params string[] args) : this(DFLT_OUT_READER, args) { }

        /// <summary>
        /// Construector.
        /// </summary>
        /// <param name="outReader">Output reader.</param>
        /// <param name="args">Arguments.</param>
        public GridProcess(IGridProcessOutputReader outReader, params string[] args)
        {
            // Add test dll path
            args = args.Concat(new[] {"-assembly=" + GetType().Assembly.Location}).ToArray();

            proc = Start(EXE_PATH, GridManager.GridGainHome(null), outReader, args);
        }

        /// <summary>
        /// Starts a grid process.
        /// </summary>
        /// <param name="exePath">Exe path.</param>
        /// <param name="ggHome">GridGain home.</param>
        /// <param name="outReader">Output reader.</param>
        /// <param name="args">Arguments.</param>
        /// <returns>Started process.</returns>
        public static Process Start(string exePath, string ggHome, IGridProcessOutputReader outReader = null, 
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
                procStart.EnvironmentVariables[GridManager.ENV_GRIDGAIN_HOME] = ggHome;

            procStart.EnvironmentVariables["GRIDGAIN_NATIVE_TEST_CLASSPATH"] = "true";

            procStart.CreateNoWindow = true;
            procStart.UseShellExecute = false;

            procStart.RedirectStandardOutput = true;
            procStart.RedirectStandardError = true;

            var workDir = Path.GetDirectoryName(exePath);

            if (workDir != null)
                procStart.WorkingDirectory = workDir;

            Console.WriteLine("About to run GridGain.exe process [exePath=" + exePath + ", arguments=" + sb + ']');

            // 2. Start.
            var proc = Process.Start(procStart);

            Debug.Assert(proc != null);

            // 3. Attach output readers to avoid hangs.
            outReader = outReader ?? DFLT_OUT_READER;

            Attach(proc, proc.StandardOutput, outReader, false);
            Attach(proc, proc.StandardError, outReader, true);

            return proc;
        }

        /// <summary>
        /// Whether the process is still alive.
        /// </summary>
        public bool Alive
        {
            get
            {
                return !proc.HasExited;
            }
        }

        /// <summary>
        /// Kill process.
        /// </summary>
        public void Kill()
        {
            proc.Kill();
        }

        /// <summary>
        /// Join process.
        /// </summary>
        /// <returns>Exit code.</returns>
        public int Join()
        {
            proc.WaitForExit();

            return proc.ExitCode;
        }

        /// <summary>
        /// Join process with timeout.
        /// </summary>
        /// <param name="timeout">Timeout in milliseconds.</param>
        /// <returns><c>True</c> if process exit occurred before timeout.</returns>
        public bool Join(int timeout)
        {
            return proc.WaitForExit(timeout);
        }

        /// <summary>
        /// Join process with timeout.
        /// </summary>
        /// <param name="timeout">Timeout in milliseconds.</param>
        /// <param name="exitCode">Exit code.</param>
        /// <returns><c>True</c> if process exit occurred before timeout.</returns>
        public bool Join(int timeout, out int exitCode)
        {
            if (proc.WaitForExit(timeout))
            {
                exitCode = proc.ExitCode;

                return true;
            }
            else
            {
                exitCode = 0;

                return false;
            }
        }

        /// <summary>
        /// Attach output reader to the process.
        /// </summary>
        /// <param name="proc">Process.</param>
        /// <param name="reader">Process stream reader.</param>
        /// <param name="outReader">Output reader.</param>
        /// <param name="err">Whether this is error stream.</param>
        private static void Attach(Process proc, StreamReader reader, IGridProcessOutputReader outReader, bool err)
        {
            Thread thread = new Thread(() =>
            {
                while (!proc.HasExited)
                    outReader.OnOutput(proc, reader.ReadLine(), err);
            }) {IsBackground = true};


            thread.Start();
        }
    }
}
