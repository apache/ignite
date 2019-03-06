/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.DotNetCore.Common
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// 'dotnet test' swallows console output. This logger writes to a file to overcome this.
    /// </summary>
    internal class TestLogger : ILogger
    {
        /** */
        public static readonly TestLogger Instance = new TestLogger();

        /** */
        private readonly StreamWriter _file;

        /// <summary>
        /// Prevents a default instance of the <see cref="TestLogger"/> class from being created.
        /// </summary>
        private TestLogger()
        {
            var binDir = Path.GetDirectoryName(GetType().Assembly.Location);
            var file = Path.Combine(binDir, "dotnet-test.log");

            if (File.Exists(file))
            {
                File.Delete(file);
            }

            _file = File.AppendText(file);
        }

        /** <inheritdoc /> */
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            lock (_file)
            {
                var text = args != null
                    ? string.Format(formatProvider ?? CultureInfo.InvariantCulture, message, args)
                    : message;

                _file.WriteLine(text);
                _file.Flush();
            }
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            return level > LogLevel.Debug;
        }
    }
}
