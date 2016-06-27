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
    using System.IO;
    using System.Text;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using NUnit.Framework;

    /// <summary>
    /// Tests that Java console output is redirected to .NET console.
    /// </summary>
    public class ConsoleRedirectTest
    {
        private StringBuilder _outSb;
        private StringBuilder _errSb;

        [SetUp]
        public void SetUp()
        {
            _outSb = new StringBuilder();
            Console.SetOut(new StringWriter(_outSb));

            _errSb = new StringBuilder();
            Console.SetError(new StringWriter(_errSb));
        }

        /// <summary>
        /// Tests the startup output.
        /// </summary>
        [Test]
        public void TestStartupOutput()
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Assert.IsTrue(_outSb.ToString().Contains("Topology snapshot ["));
            }
        }

        /// <summary>
        /// Tests startup error in Java.
        /// </summary>
        [Test]
        public void TestStartupJavaError()
        {
            // Invalid config
            Assert.Throws<IgniteException>(() =>
                Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    CommunicationSpi = new TcpCommunicationSpi
                    {
                        IdleConnectionTimeout = TimeSpan.MinValue
                    }
                }));

            Assert.IsTrue(_errSb.ToString().Contains("SPI parameter failed condition check: idleConnTimeout > 0"));
        }
    }
}
