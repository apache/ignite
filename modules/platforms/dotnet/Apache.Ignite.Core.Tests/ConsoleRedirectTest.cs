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
    using System.Text.RegularExpressions;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.Tests.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests that Java console output is redirected to .NET console.
    /// </summary>
    public class ConsoleRedirectTest
    {
        /** Thread name task name. */
        private const string ConsoleWriteTask = "org.apache.ignite.platform.PlatformConsoleWriteTask";

        /** */
        private StringBuilder _outSb;

        /** */
        private StringBuilder _errSb;

        /** */
        private TextWriter _stdOut;

        /** */
        private TextWriter _stdErr;

        /// <summary>
        /// Sets up the test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            _stdOut = Console.Out;
            _stdErr = Console.Error;

            _outSb = new StringBuilder();
            Console.SetOut(new MyStringWriter(_outSb));

            _errSb = new StringBuilder();
            Console.SetError(new MyStringWriter(_errSb));
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            MyStringWriter.Throw = false;

            Console.SetOut(_stdOut);
            Console.SetError(_stdErr);

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the startup output.
        /// </summary>
        [Test]
        public void TestStartupOutput()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                Logger = null
            };
            
            using (Ignition.Start(cfg))
            {
                Assert.AreEqual(1, Regex.Matches(_outSb.ToString(), "ver=1, locNode=[a-fA-F0-9]{8,8}, servers=1, clients=0,").Count);
            }
        }

        /// <summary>
        /// Tests the exception in console writer.
        /// </summary>
        [Test]
        public void TestExceptionInWriterPropagatesToJavaAndBack()
        {
            MyStringWriter.Throw = true;

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(TestUtils.GetTestConfiguration()));
            Assert.AreEqual("foo", ex.Message);
        }

        [Test]
        public void TestConsoleWriteSpecialStrings()
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            foreach (var val in BinarySelfTest.SpecialStrings)
            {
                MyStringWriter.LastValue = null;

                // Send to Java as UTF-16 to avoid dealing with IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2
                var bytes = Encoding.Unicode.GetBytes(MyStringWriter.Prefix + val);
                ignite.GetCompute().ExecuteJavaTask<string>(ConsoleWriteTask, bytes);

                var expectedStr = GetExpectedStr(val);
                Assert.AreEqual(expectedStr, MyStringWriter.LastValue, message: val);
                StringAssert.Contains(expectedStr, _outSb.ToString(), message: val);

                // Test Env.NewString
                MyStringWriter.LastValue = null;
                TestUtilsJni.Println(MyStringWriter.Prefix + val);

                Assert.AreEqual(expectedStr.Length, MyStringWriter.LastValue?.Length, message: val);
                if (val != BinarySelfTest.SpecialStrings[0])
                {
                    Assert.AreEqual(expectedStr, MyStringWriter.LastValue, message: val);
                }
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
                    },
                    Logger = null
                }));

            StringAssert.Contains("SPI parameter failed condition check: idleConnTimeout > 0", _errSb.ToString());
        }

#if !NETCOREAPP
        /// <summary>
        /// Tests the disabled redirect.
        /// </summary>
        [Test]
        public void TestDisabledRedirect()
        {
            // Run test in new process because JVM is initialized only once.
            const string envVar = "ConsoleRedirectTest.TestDisabledRedirect";

            if (Environment.GetEnvironmentVariable(envVar) == bool.TrueString)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(false))
                {
                    Logger = null
                };
                
                Assert.IsTrue(cfg.RedirectJavaConsoleOutput);

                cfg.RedirectJavaConsoleOutput = false;

                using (Ignition.Start(cfg))
                {
                    Assert.AreEqual(string.Empty, _errSb.ToString());
                    Assert.AreEqual(string.Empty, _outSb.ToString());
                }
            }
            else
            {
                using (EnvVar.Set(envVar, bool.TrueString))
                {
                    TestUtils.RunTestInNewProcess(GetType().FullName, "TestDisabledRedirect");
                }
            }
        }

        /// <summary>
        /// Tests multiple appdomains and multiple console handlers.
        /// </summary>
        [Test]
        public void TestMultipleDomains()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(noLogger: true));
            
            using (var ignite = Ignition.Start(cfg))
            {
                Assert.AreEqual(1, Regex.Matches(_outSb.ToString(), "ver=1, locNode=[a-fA-F0-9]{8,8}, servers=1, clients=0,").Count);

                // Run twice
                RunInNewDomain();
                RunInNewDomain();

                Assert.AreEqual(5, ignite.GetCluster().TopologyVersion);

                var outTxt = _outSb.ToString();

                // Check output from another domain (2 started + 2 stopped = 4)
                Assert.AreEqual(4, Regex.Matches(outTxt, ">>> Ignite instance name: newDomainGrid").Count);

                // Both domains produce the topology snapshot on node enter
                Assert.AreEqual(2, Regex.Matches(outTxt, "ver=2, locNode=[a-fA-F0-9]{8,8}, servers=2, clients=0,").Count);
                Assert.AreEqual(1, Regex.Matches(outTxt, "ver=3, locNode=[a-fA-F0-9]{8,8}, servers=1, clients=0,").Count);
                Assert.AreEqual(2, Regex.Matches(outTxt, "ver=4, locNode=[a-fA-F0-9]{8,8}, servers=2, clients=0,").Count);
                Assert.AreEqual(1, Regex.Matches(outTxt, "ver=5, locNode=[a-fA-F0-9]{8,8}, servers=1, clients=0,").Count);
            }
        }

        /// <summary>
        /// Runs the Ignite in a new domain.
        /// </summary>
        private static void RunInNewDomain()
        {
            AppDomain childDomain = null;

            try
            {
                childDomain = AppDomain.CreateDomain("Child", null, new AppDomainSetup
                {
                    ApplicationBase = AppDomain.CurrentDomain.SetupInformation.ApplicationBase,
                    ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile,
                    ApplicationName = AppDomain.CurrentDomain.SetupInformation.ApplicationName,
                    LoaderOptimization = LoaderOptimization.MultiDomainHost
                });

                var type = typeof(IgniteRunner);
                Assert.IsNotNull(type.FullName);

                var runner = (IIgniteRunner)childDomain.CreateInstanceAndUnwrap(
                    type.Assembly.FullName, type.FullName);

                runner.Run();
            }
            finally
            {
                if (childDomain != null)
                    AppDomain.Unload(childDomain);
            }
        }

        private interface IIgniteRunner
        {
            void Run();
        }

        private class IgniteRunner : MarshalByRefObject, IIgniteRunner
        {
            public void Run()
            {
                Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration(noLogger: true))
                {
                    IgniteInstanceName = "newDomainGrid"
                });

                // Will be stopped automatically on domain unload.
            }
        }
#endif

        private static string GetExpectedStr(string val)
        {
            if (val != BinarySelfTest.SpecialStrings[0])
            {
                return val;
            }

            // Some special strings are not equal to themselves after UTF16 roundtrip,
            // even though they contain exactly the same bytes.
            return Encoding.Unicode.GetString(Encoding.Unicode.GetBytes(val));
        }

        private class MyStringWriter : StringWriter
        {
            public const string Prefix = "[MyStringWriter]";

            public static bool Throw { get; set; }

            public static string LastValue { get; set; }

            public MyStringWriter(StringBuilder sb) : base(sb)
            {
                // No-op.
            }

            public override void Write(string value)
            {
                if (Throw)
                {
                    throw new Exception("foo");
                }

                base.Write(value);

                if (!string.IsNullOrWhiteSpace(value) && value.StartsWith(Prefix))
                {
                    LastValue = value.Substring(Prefix.Length);
                }
            }
        }
    }
}
