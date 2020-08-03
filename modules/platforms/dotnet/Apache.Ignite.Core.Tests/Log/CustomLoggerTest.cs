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

namespace Apache.Ignite.Core.Tests.Log
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Communication.Tcp;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Resource;
    using NUnit.Framework;

    /// <summary>
    /// Tests that user-defined logger receives Ignite events.
    /// </summary>
    public class CustomLoggerTest
    {
        /** */
        private static readonly LogLevel[] AllLevels = Enum.GetValues(typeof (LogLevel)).OfType<LogLevel>().ToArray();

        /// <summary>
        /// Test setup.
        /// </summary>
        [SetUp]
        public void TestSetUp()
        {
            TestLogger.Clear();
        }

        /// <summary>
        /// Tests the startup output.
        /// </summary>
        [Test]
        public void TestStartupOutput()
        {
            var cfg = GetConfigWithLogger(true);
            using (var ignite = Ignition.Start(cfg))
            {
                // Check injection
                Assert.AreEqual(ignite, ((TestLogger) cfg.Logger).Ignite);

                // Check initial message
                Assert.IsTrue(TestLogger.Entries[0].Message.StartsWith("Starting Ignite.NET"));

                // Check topology message
                Assert.IsTrue(
                    TestUtils.WaitForCondition(() =>
                    {
                        lock (TestLogger.Entries)
                        {
                            return TestLogger.Entries.Any(x => x.Message.Contains("Topology snapshot"));
                        }
                    }, 9000), "No topology snapshot");
            }

            // Test that all levels are present
            foreach (var level in AllLevels.Where(x => x != LogLevel.Error))
            {
                Assert.IsTrue(TestLogger.Entries.Any(x => x.Level == level), "No messages with level " + level);
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
                Ignition.Start(new IgniteConfiguration(GetConfigWithLogger())
                {
                    CommunicationSpi = new TcpCommunicationSpi
                    {
                        IdleConnectionTimeout = TimeSpan.MinValue
                    }
                }));

            var err = TestLogger.Entries.First(x => x.Level == LogLevel.Error);
            Assert.IsTrue(err.NativeErrorInfo.Contains("SPI parameter failed condition check: idleConnTimeout > 0"));
            Assert.AreEqual("org.apache.ignite.internal.IgniteKernal", err.Category);
            Assert.IsNull(err.Exception);
        }

        /// <summary>
        /// Tests startup error in .NET.
        /// </summary>
        [Test]
        public void TestStartupDotNetError()
        {
            // Invalid bean
            Assert.Throws<IgniteException>(() =>
                Ignition.Start(new IgniteConfiguration(GetConfigWithLogger())
                {
                    LifecycleHandlers = new[] {new FailBean()}
                }));

            var err = TestLogger.Entries.First(x => x.Level == LogLevel.Error);
            Assert.IsInstanceOf<ArithmeticException>(err.Exception);
        }

        /// <summary>
        /// Tests that .NET exception propagates through Java to the log.
        /// </summary>
        [Test]
        public void TestDotNetErrorPropagation()
        {
            // Start 2 nodes: PlatformNativeException does not occur in local scenario
            using (var ignite = Ignition.Start(GetConfigWithLogger()))
            using (Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration()) {IgniteInstanceName = "1"}))
            {
                var compute = ignite.GetCluster().ForRemotes().GetCompute();

                var ex = Assert.Throws<AggregateException>(() => compute.Call(new FailFunc()));
                Assert.IsNotNull(ex.InnerException);
                Assert.IsInstanceOf<ArithmeticException>(ex.InnerException.InnerException);

                // Log updates may not arrive immediately
                TestUtils.WaitForCondition(() => TestLogger.Entries.Any(x => x.Exception != null), 3000);

                var errFromJava = TestLogger.Entries.Single(x => x.Exception != null);
                Assert.IsNotNull(errFromJava.Exception.InnerException);
                Assert.AreEqual("Error in func.", 
                    ((ArithmeticException) errFromJava.Exception.InnerException).Message);
            }
        }

        /// <summary>
        /// Tests the <see cref="QueryEntity"/> validation.
        /// </summary>
        [Test]
        public void TestQueryEntityValidation()
        {
            var cacheCfg = new CacheConfiguration("cache1", new QueryEntity(typeof(uint), typeof(ulong))
            {
                Fields = new[]
                {
                    new QueryField("myField", typeof(ushort))
                }
            });

            var cfg = new IgniteConfiguration(GetConfigWithLogger())
            {
                CacheConfiguration = new[]
                {
                    cacheCfg
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                // Check static and dynamic cache start
                cacheCfg.Name = "cache2";
                ignite.CreateCache<int, string>(cacheCfg);

                var warns = TestLogger.Entries.Where(x => x.Level == LogLevel.Warn && x.Args != null)
                    .Select(x => string.Format(x.Message, x.Args)).ToList();

                Assert.AreEqual(6, warns.Count);

                Assert.AreEqual("Validating cache configuration 'cache1', QueryEntity 'java.lang.Integer:java.lang." +
                                "Long': Type 'System.UInt32' maps to Java type 'java.lang.Integer' using unchecked " +
                                "conversion. This may cause issues in SQL queries. You can use 'System.Int32' " +
                                "instead to achieve direct mapping.", warns[0]);

                Assert.AreEqual("Validating cache configuration 'cache1', QueryEntity 'java.lang.Integer:java.lang." +
                                "Long': Type 'System.UInt64' maps to Java type 'java.lang.Long' using unchecked " +
                                "conversion. This may cause issues in SQL queries. You can use 'System.Int64' " +
                                "instead to achieve direct mapping.", warns[1]);

                Assert.AreEqual("Validating cache configuration 'cache1', QueryEntity 'java.lang.Integer:java.lang." +
                                "Long', QueryField 'myField': Type 'System.UInt16' maps to Java type 'java.lang." +
                                "Short' using unchecked conversion. This may cause issues in SQL queries. You " +
                                "can use 'System.Int16' instead to achieve direct mapping.", warns[2]);

                Assert.AreEqual("Validating cache configuration 'cache2', QueryEntity 'java.lang.Integer:java.lang." +
                                "Long': Type 'System.UInt32' maps to Java type 'java.lang.Integer' using unchecked " +
                                "conversion. This may cause issues in SQL queries. You can use 'System.Int32' " +
                                "instead to achieve direct mapping.", warns[3]);

                Assert.AreEqual("Validating cache configuration 'cache2', QueryEntity 'java.lang.Integer:java.lang." +
                                "Long': Type 'System.UInt64' maps to Java type 'java.lang.Long' using unchecked " +
                                "conversion. This may cause issues in SQL queries. You can use 'System.Int64' " +
                                "instead to achieve direct mapping.", warns[4]);

                Assert.AreEqual("Validating cache configuration 'cache2', QueryEntity 'java.lang.Integer:java.lang." +
                                "Long', QueryField 'myField': Type 'System.UInt16' maps to Java type 'java.lang." +
                                "Short' using unchecked conversion. This may cause issues in SQL queries. You " +
                                "can use 'System.Int16' instead to achieve direct mapping.", warns[5]);
            }
        }

        /// <summary>
        /// Tests the <see cref="LoggerExtensions"/> methods.
        /// </summary>
        [Test]
        public void TestExtensions()
        {
            var log = new TestLogger(LogLevel.Trace);
            var ex = new FieldAccessException("abc");

            // Log
            log.Log(LogLevel.Trace, "trace");
            CheckLastMessage(LogLevel.Trace, "trace");

            log.Log(LogLevel.Debug, "msg {0} {1}", 1, "2");
            CheckLastMessage(LogLevel.Debug, "msg {0} {1}", new object[] { 1, "2" }, CultureInfo.InvariantCulture);

            log.Log(LogLevel.Info, ex, "msg");
            CheckLastMessage(LogLevel.Info, "msg", e: ex);

            log.Log(LogLevel.Warn, ex, "msg {0}", 1);
            CheckLastMessage(LogLevel.Warn, "msg {0}", new object[] {1}, CultureInfo.InvariantCulture, e: ex);

            // Trace
            log.Trace("trace");
            CheckLastMessage(LogLevel.Trace, "trace");

            log.Trace("msg {0} {1}", 1, "2");
            CheckLastMessage(LogLevel.Trace, "msg {0} {1}", new object[] { 1, "2" }, CultureInfo.InvariantCulture);

            log.Trace(ex, "msg");
            CheckLastMessage(LogLevel.Trace, "msg", e: ex);

            log.Trace(ex, "msg {0}", 1);
            CheckLastMessage(LogLevel.Trace, "msg {0}", new object[] { 1 }, CultureInfo.InvariantCulture, e: ex);

            // Debug
            log.Debug("test");
            CheckLastMessage(LogLevel.Debug, "test");

            log.Debug("msg {0} {1}", 1, "2");
            CheckLastMessage(LogLevel.Debug, "msg {0} {1}", new object[] { 1, "2" }, CultureInfo.InvariantCulture);

            log.Debug(ex, "msg");
            CheckLastMessage(LogLevel.Debug, "msg", e: ex);

            log.Debug(ex, "msg {0}", 1);
            CheckLastMessage(LogLevel.Debug, "msg {0}", new object[] { 1 }, CultureInfo.InvariantCulture, e: ex);

            // Info
            log.Info("test");
            CheckLastMessage(LogLevel.Info, "test");

            log.Info("msg {0} {1}", 1, "2");
            CheckLastMessage(LogLevel.Info, "msg {0} {1}", new object[] { 1, "2" }, CultureInfo.InvariantCulture);

            log.Info(ex, "msg");
            CheckLastMessage(LogLevel.Info, "msg", e: ex);

            log.Info(ex, "msg {0}", 1);
            CheckLastMessage(LogLevel.Info, "msg {0}", new object[] { 1 }, CultureInfo.InvariantCulture, e: ex);

            // Warn
            log.Warn("test");
            CheckLastMessage(LogLevel.Warn, "test");

            log.Warn("msg {0} {1}", 1, "2");
            CheckLastMessage(LogLevel.Warn, "msg {0} {1}", new object[] { 1, "2" }, CultureInfo.InvariantCulture);

            log.Warn(ex, "msg");
            CheckLastMessage(LogLevel.Warn, "msg", e: ex);

            log.Warn(ex, "msg {0}", 1);
            CheckLastMessage(LogLevel.Warn, "msg {0}", new object[] { 1 }, CultureInfo.InvariantCulture, e: ex);

            // Error
            log.Error("test");
            CheckLastMessage(LogLevel.Error, "test");

            log.Error("msg {0} {1}", 1, "2");
            CheckLastMessage(LogLevel.Error, "msg {0} {1}", new object[] { 1, "2" }, CultureInfo.InvariantCulture);

            log.Error(ex, "msg");
            CheckLastMessage(LogLevel.Error, "msg", e: ex);

            log.Error(ex, "msg {0}", 1);
            CheckLastMessage(LogLevel.Error, "msg {0}", new object[] { 1 }, CultureInfo.InvariantCulture, e: ex);

            // GetLogger
            var catLog = log.GetLogger("myCategory");
            catLog.Info("info");
            CheckLastMessage(LogLevel.Info, "info", category: "myCategory");

            catLog.Log(LogLevel.Info, "info", null, null, "explicitCat", null, null);
            CheckLastMessage(LogLevel.Info, "info", category: "explicitCat");

            catLog = catLog.GetLogger("newCat");
            catLog.Info("info");
            CheckLastMessage(LogLevel.Info, "info", category: "newCat");

            catLog.Log(LogLevel.Info, "info", null, null, "explicitCat", null, null);
            CheckLastMessage(LogLevel.Info, "info", category: "explicitCat");
        }

        /// <summary>
        /// Checks the last message.
        /// </summary>
        private static void CheckLastMessage(LogLevel level, string message, object[] args = null, 
            IFormatProvider formatProvider = null, string category = null, string nativeErr = null, Exception e = null)
        {
            var msg = TestLogger.Entries.Last();

            Assert.AreEqual(msg.Level, level);
            Assert.AreEqual(msg.Message, message);
            Assert.AreEqual(msg.Args, args);
            Assert.AreEqual(msg.FormatProvider, formatProvider);
            Assert.AreEqual(msg.Category, category);
            Assert.AreEqual(msg.NativeErrorInfo, nativeErr);
            Assert.AreEqual(msg.Exception, e);
        }

        /// <summary>
        /// Gets the configuration with logger.
        /// </summary>
        private static IgniteConfiguration GetConfigWithLogger(bool verbose = false)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                Logger = new TestLogger(verbose ? LogLevel.Trace : LogLevel.Info)
            };
        }

        /// <summary>
        /// Test log entry.
        /// </summary>
        private class LogEntry
        {
            public LogLevel Level;
            public string Message;
            public object[] Args;
            public IFormatProvider FormatProvider;
            public string Category;
            public string NativeErrorInfo;
            public Exception Exception;

            public override string ToString()
            {
                return string.Format("Level: {0}, Message: {1}, Args: {2}, FormatProvider: {3}, Category: {4}, " +
                                     "NativeErrorInfo: {5}, Exception: {6}", Level, Message, Args, FormatProvider, 
                                     Category, NativeErrorInfo, Exception);
            }
        }

        /// <summary>
        /// Test logger.
        /// </summary>
        private class TestLogger : ILogger
        {
            private static readonly List<LogEntry> Logs = new List<LogEntry>(5000);

            private readonly LogLevel _minLevel;

            public TestLogger(LogLevel minLevel)
            {
                _minLevel = minLevel;
            }

            public static List<LogEntry> Entries
            {
                get
                {
                    lock (Logs)
                    {
                        return Logs.ToList();
                    }
                }
            }

            public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, 
                string category, string nativeErrorInfo, Exception ex)
            {
                if (!IsEnabled(level))
                    return;

                lock (Logs)
                {
                    Logs.Add(new LogEntry
                    {
                        Level = level,
                        Message = message,
                        Args = args,
                        FormatProvider = formatProvider,
                        Category = category,
                        NativeErrorInfo = nativeErrorInfo,
                        Exception = ex
                    });
                }
            }

            public bool IsEnabled(LogLevel level)
            {
                return level >= _minLevel;
            }

            [InstanceResource]
            // ReSharper disable once UnusedAutoPropertyAccessor.Local
            public IIgnite Ignite { get; set; }

            public static void Clear()
            {
                lock (Logs)
                {
                    Logs.Clear();
                }
            }
        }


        /// <summary>
        /// Failing lifecycle bean.
        /// </summary>
        private class FailBean : ILifecycleHandler
        {
            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                throw new ArithmeticException("Failure in bean");
            }
        }

        /// <summary>
        /// Failing computation.
        /// </summary>
        [Serializable]
        private class FailFunc : IComputeFunc<string>
        {
            public string Invoke()
            {
                throw new ArithmeticException("Error in func.");
            }
        }
    }
}
