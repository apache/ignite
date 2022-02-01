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
        public static string ExecuteSafe(string file, string args, int timeoutMs = 1000)
        {
            try
            {
                var processStartInfo = new ProcessStartInfo
                {
                    FileName = file,
                    Arguments = args,
                    RedirectStandardOutput = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                using (var process = new Process {StartInfo = processStartInfo})
                {
                    process.Start();

                    if (!process.WaitForExit(timeoutMs))
                    {
                        process.Kill();
                    }

                    return process.StandardOutput.ReadToEnd();
                }
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }
    }
}
