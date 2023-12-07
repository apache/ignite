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
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Text;
    using System.Threading;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Shell utils (cmd/bash).
    /// </summary>
    [ExcludeFromCodeCoverage]
    internal static class Shell
    {
        /// <summary>
        /// Executes the command.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "ExecuteSafe should ignore all exceptions.")]
        public static string ExecuteSafe(string file, string args, int timeoutMs = 1000, ILogger log = null)
        {
            try
            {
                var processStartInfo = new ProcessStartInfo
                {
                    FileName = file,
                    Arguments = args,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                var stdOut = new StringBuilder();
                var stdErr = new StringBuilder();

                using (var stdOutEvt = new ManualResetEventSlim())
                using (var stdErrEvt = new ManualResetEventSlim())
                using (var process = new Process {StartInfo = processStartInfo})
                {
                    process.OutputDataReceived += (_, e) =>
                    {
                        if (e.Data == null)
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            stdOutEvt.Set();
                        }
                        else
                        {
                            stdOut.AppendLine(e.Data);
                        }
                    };

                    process.ErrorDataReceived += (_, e) =>
                    {
                        if (e.Data == null)
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            stdErrEvt.Set();
                        }
                        else
                        {
                            stdErr.AppendLine(e.Data);
                        }
                    };

                    process.Start();

                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();

                    if (!process.WaitForExit(timeoutMs))
                    {
                        log?.Warn("Shell command '{0}' timed out.", file);

                        process.Kill();
                    }

                    if (!stdOutEvt.Wait(timeoutMs) || !stdErrEvt.Wait(timeoutMs))
                    {
                        log?.Warn("Shell command '{0}' timed out when waiting for stdout/stderr.", file);
                    }

                    if (stdErr.Length > 0)
                    {
                        log?.Warn("Shell command '{0}' stderr: '{1}'", file, stdErr);
                    }

                    if (process.ExitCode != 0)
                    {
                        log?.Warn("Shell command '{0}' exit code: {1}", file, process.ExitCode);
                    }

                    return stdOut.ToString();
                }
            }
            catch (Exception e)
            {
                log?.Warn("Shell command '{0}' failed: '{1}'", file, e.Message, e);

                return string.Empty;
            }
        }
    }
}
