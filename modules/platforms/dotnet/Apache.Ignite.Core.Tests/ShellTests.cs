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
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="Shell"/> class.
    /// </summary>
    public class ShellTests
    {
        /// <summary>
        /// Tests <see cref="Shell.ExecuteSafe"/> method.
        /// </summary>
        [Test]
        // [Explicit] // TODO: Does this time out on TC? Why?
        public void TestExecuteSafe()
        {
            if (Os.IsWindows)
            {
                return;
            }
            
            TestContext.Progress.WriteLine(">>> TestExecuteSafe 1");

            var uname = Shell.ExecuteSafe("uname", string.Empty);
            TestContext.Progress.WriteLine(">>> TestExecuteSafe 2 " + uname);
            Assert.IsNotEmpty(uname, uname);
            Console.WriteLine(uname);

            var readlink = Shell.ExecuteSafe("readlink", "-f /usr/bin/java");
            TestContext.Progress.WriteLine(">>> TestExecuteSafe 5 " + readlink);
            Assert.IsNotEmpty(readlink, readlink);
            Console.WriteLine(readlink);
            
            if (Os.IsLinux)
            {
                Assert.AreEqual("Linux", uname.Trim());
            }

            TestContext.Progress.WriteLine(">>> TestExecuteSafe 6");
            Assert.IsEmpty(Shell.ExecuteSafe("readlink", "-foobar"));
            TestContext.Progress.WriteLine(">>> TestExecuteSafe 7");
            Assert.IsEmpty(Shell.ExecuteSafe("foo_bar", "abc"));
            TestContext.Progress.WriteLine(">>> TestExecuteSafe 8");
        }
    }
}