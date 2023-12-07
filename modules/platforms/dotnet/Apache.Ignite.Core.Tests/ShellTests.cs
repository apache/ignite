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
    using System.Linq;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="Shell"/> class.
    /// </summary>
    [Platform("Linux")]
    [Order(1)] // Execute first to avoid zombie processes (see https://issues.apache.org/jira/browse/IGNITE-13536).
    public class ShellTests
    {
        /// <summary>
        /// Tests <see cref="Shell.ExecuteSafe"/> method.
        /// </summary>
        [Test]
        public void TestExecuteSafeReturnsStdout()
        {
            var log = GetLogger();

            var uname = Shell.ExecuteSafe("uname", string.Empty, log: log);
            Assert.AreEqual("Linux", uname.Trim(), string.Join(", ", log.Entries.Select(e => e.ToString())));
            Assert.IsEmpty(log.Entries);

            var readlink = Shell.ExecuteSafe("readlink", "-f /usr/bin/java", log: log);
            Assert.IsNotEmpty(readlink, readlink, string.Join(", ", log.Entries.Select(e => e.ToString())));
            Assert.IsEmpty(log.Entries);
        }

        /// <summary>
        /// Tests that non-zero error code and stderr are logged.
        /// </summary>
        [Test]
        public void TestExecuteSafeLogsNonZeroExitCodeAndStderr()
        {
            var log = GetLogger();
            var res = Shell.ExecuteSafe("uname", "--badarg", log: log);

            var entries = log.Entries;
            var entriesString = string.Join(", ", entries.Select(e => e.ToString()));

            Assert.IsEmpty(res, entriesString);
            Assert.AreEqual(2, entries.Count, entriesString);

            Assert.AreEqual(LogLevel.Warn, entries[0].Level);
            StringAssert.StartsWith("Shell command 'uname' stderr: 'uname: unrecognized option", entries[0].Message);

            Assert.AreEqual(LogLevel.Warn, entries[1].Level);
            Assert.AreEqual("Shell command 'uname' exit code: 1", entries[1].Message);
        }

        /// <summary>
        /// Tests that failure to execute a command is logged.
        /// </summary>
        [Test]
        public void TestExecuteSafeLogsException()
        {
            var log = GetLogger();
            var res = Shell.ExecuteSafe("foo_bar", "abc", log: log);
            var entries = log.Entries;

            Assert.IsEmpty(res);
            Assert.AreEqual(1, entries.Count);

            Assert.AreEqual(LogLevel.Warn, entries[0].Level);
            Assert.AreEqual("Shell command 'foo_bar' failed: 'No such file or directory'", entries[0].Message);
        }

        private static ListLogger GetLogger()
        {
            return new ListLogger
            {
                EnabledLevels = Enum.GetValues(typeof(LogLevel)).Cast<LogLevel>().ToArray()
            };
        }
    }
}
