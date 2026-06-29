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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    /// <summary>
    /// Reads process output into a list.
    /// </summary>
    public class ListDataReader : IIgniteProcessOutputReader
    {
        /** JVM warnings that can appear before the actual Apache.Ignite.exe output. */
        private static readonly string[] IgnoredJavaWarnings =
        {
            "OpenJDK 64-Bit Server VM warning: Ignoring option --illegal-access=permit; support was removed in 17.0",
            "WARNING: An illegal reflective access operation has occurred",
            "WARNING: Illegal reflective access by org.apache.ignite.internal.util.GridUnsafe$2",
            "WARNING: Please consider reporting this to the maintainers of org.apache.ignite.internal.util.GridUnsafe$2",
            "WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations",
            "WARNING: All illegal access operations will be denied in a future release"
        };

        /** Target list. */
        private readonly List<string> _list = new List<string>();

        /** <inheritdoc /> */
        public void OnOutput(Process proc, string data, bool err)
        {
            if (string.IsNullOrEmpty(data))
            {
                return;
            }

            lock (_list)
            {
                _list.Add(data);
            }
        }

        /// <summary>
        /// Gets the output.
        /// </summary>
        public IList<string> GetOutput()
        {
            lock (_list)
            {
                return _list.ToList();
            }
        }

        /// <summary>
        /// Gets the output without known JVM warnings.
        /// </summary>
        public IList<string> GetOutputWithoutJavaWarnings()
        {
            return GetOutput().Where(x => !IsIgnoredJavaWarning(x)).ToList();
        }

        /// <summary>
        /// Returns a value indicating whether provided message is a known JVM warning.
        /// </summary>
        private static bool IsIgnoredJavaWarning(string message)
        {
            return IgnoredJavaWarnings.Any(warning => message.StartsWith(warning, StringComparison.Ordinal));
        }
    }
}
