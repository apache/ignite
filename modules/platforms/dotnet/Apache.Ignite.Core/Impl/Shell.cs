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
        public static string ExecuteSafe(string file, string args)
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

                var sb = new StringBuilder();

                using (var process = new Process {StartInfo = processStartInfo})
                {
                    process.OutputDataReceived += (_, eventArgs) =>
                    {
                        sb.Append(eventArgs.Data);
                    };

                    process.ErrorDataReceived += (_, eventArgs) =>
                    {
                        sb.Append(eventArgs.Data);
                    };

                    process.Start();
                    process.BeginOutputReadLine();

                    // TODO: Looks like a problem with Java signal handlers and processes that we had in tests.
                    if (!process.WaitForExit(3000))
                    {
                        process.Kill();
                    }

                    return sb.ToString();
                }
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }
    }
}
