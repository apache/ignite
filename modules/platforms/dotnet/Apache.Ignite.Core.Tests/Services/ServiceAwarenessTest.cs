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

namespace Apache.Ignite.Core.Tests.Services
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Services tests.
    /// </summary>
#pragma warning disable CS0618 // Method is obsolete.
    public class ServicesAwarenessTest
    {
        /** */
        private const string SvcName = "Service1";

        /** */
        private const string PlatformSvcName = "PlatformTestService";

        // /** Service method call counter. */
        // private static int _serviceCallCounter;

        /** */
        protected IIgnite Grid1;

        /** */
        protected IIgnite Grid2;

        /** */
        protected IIgnite Grid3;


        /** */
        private IIgniteClient _thinClient;

        /** */
        protected IIgnite[] Grids;

        /* Deploy Java service name. */
        private string _javaSvcName;

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            StopGrids();
        }

        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            StartGrids();

            _thinClient = Ignition.StartClient(GetClientConfiguration());

            var cfg = new ServiceConfiguration
            {
                Name = PlatformSvcName,
                MaxPerNodeCount = 1,
                TotalCount = 1,
                NodeFilter = new NodeIdFilter {NodeId = Grid1.GetCluster().GetLocalNode().Id},
                Service = new PlatformTestService(),
                Interceptors = new List<IServiceCallInterceptor>
                    {new PlatformTestServiceInterceptor("testInterception")}
            };

            Services.Deploy(cfg);

            _javaSvcName = TestUtils.DeployJavaService(Grid1);
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                Services.CancelAll();
            }
            catch (Exception)
            {
                // Restart grids to cleanup
                StopGrids();

                throw;
            }
            finally
            {
                if (TestContext.CurrentContext.Test.Name.StartsWith("TestEventTypes"))
                    StopGrids(); // clean events for other tests
            }
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (Grid1 != null)
                return;

            var path = Path.Combine("Config", "Compute", "compute-grid");
            Grid1 = Ignition.Start(GetConfiguration(path + "1.xml"));
            Grid2 = Ignition.Start(GetConfiguration(path + "2.xml"));

            var cfg = GetConfiguration(path + "3.xml");

            Grid3 = Ignition.Start(cfg);

            cfg.ClientMode = true;
            cfg.IgniteInstanceName = "client";

            Grids = new[] {Grid1, Grid2, Grid3};
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            Grid1 = Grid2 = Grid3 = null;
            Grids = null;

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springConfigUrl,
                LifecycleHandlers = null
            };
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        private IgniteClientConfiguration GetClientConfiguration()
        {
            var port = IgniteClientConfiguration.DefaultPort;

            return new IgniteClientConfiguration
            {
                Endpoints = new List<string> {IPAddress.Loopback + ":" + port},
                SocketTimeout = TimeSpan.FromSeconds(15),
                Logger = new ListLogger(new ConsoleLogger {MinLevel = LogLevel.Trace}),
            };
        }

        /// <summary>
        /// Gets the services.
        /// </summary>
        protected virtual IServices Services
        {
            get { return Grid1.GetServices(); }
        }

        /// <summary>
        /// Makes Service1-{i} names for services.
        /// </summary>
        private static string MakeServiceName(int i)
        {
            // Please note that CheckContext() validates Name.StartsWith(SvcName).
            return string.Format("{0}-{1}", SvcName, i);
        }

        /// <summary>
        /// Tests Java service invocation via thin client.
        /// </summary>
        [Test]
        public void TestCallJavaServiceThinClient()
        {
            var svc = _thinClient.GetServices().GetServiceProxy<IJavaService>(_javaSvcName, callContext());
            var binSvc = _thinClient.GetServices().WithKeepBinary().WithServerKeepBinary()
                .GetServiceProxy<IJavaService>(_javaSvcName, callContext());

            DoAllServiceTests(svc, binSvc, false, false, false);
        }

        /// <summary>
        /// Tests service invocation.
        /// </summary>
        private void DoAllServiceTests(IJavaService svc, IJavaService binSvc, bool isClient, bool isPlatform,
            bool checkException = true)
        {
            DoTestService(svc);
        }
        
        /// <summary>
        /// Test node filter.
        /// </summary>
        [Serializable]
        private class NodeIdFilter : IClusterNodeFilter
        {
            /// <summary>
            /// Gets or sets the node identifier.
            /// </summary>
            public Guid NodeId { get; set; }

            /** <inheritdoc /> */
            public bool Invoke(IClusterNode node)
            {
                return node.Id == NodeId;
            }
        }

        /// <summary>
        /// Tests service methods.
        /// </summary>
        private void DoTestService(IJavaService svc)
        {
            // Basics
            Assert.IsTrue(svc.isInitialized());
            Assert.IsTrue(TestUtils.WaitForCondition(() => svc.isExecuted(), 500));
            Assert.IsFalse(svc.isCancelled());

            // Primitives
            Assert.AreEqual(4, svc.test((byte) 3));
            Assert.AreEqual(5, svc.test((short) 4));
            Assert.AreEqual(6, svc.test(5));
            Assert.AreEqual(6, svc.test((long) 5));
            Assert.AreEqual(3.8f, svc.test(2.3f));
            Assert.AreEqual(5.8, svc.test(3.3));
            Assert.IsFalse(svc.test(true));
            Assert.AreEqual('b', svc.test('a'));
            Assert.AreEqual("Foo!", svc.test("Foo"));

            // Nullables (Java wrapper types)
            Assert.AreEqual(4, svc.testWrapper(3));
            Assert.AreEqual(5, svc.testWrapper((short?) 4));
            Assert.AreEqual(6, svc.testWrapper((int?) 5));
            Assert.AreEqual(6, svc.testWrapper((long?) 5));
            Assert.AreEqual(3.8f, svc.testWrapper(2.3f));
            Assert.AreEqual(5.8, svc.testWrapper(3.3));
            Assert.AreEqual(false, svc.testWrapper(true));
            Assert.AreEqual('b', svc.testWrapper('a'));

            // Arrays
            var bytes = svc.testArray(new byte[] {1, 2, 3});

            Assert.AreEqual(bytes.GetType(), typeof(byte[]));
            Assert.AreEqual(new byte[] {2, 3, 4}, bytes);

            var shorts = svc.testArray(new short[] {1, 2, 3});

            Assert.AreEqual(shorts.GetType(), typeof(short[]));
            Assert.AreEqual(new short[] {2, 3, 4}, shorts);

            var ints = svc.testArray(new[] {1, 2, 3});

            Assert.AreEqual(ints.GetType(), typeof(int[]));
            Assert.AreEqual(new[] {2, 3, 4}, ints);

            var longs = svc.testArray(new long[] {1, 2, 3});

            Assert.AreEqual(longs.GetType(), typeof(long[]));
            Assert.AreEqual(new long[] {2, 3, 4}, longs);

            var floats = svc.testArray(new float[] {1, 2, 3});

            Assert.AreEqual(floats.GetType(), typeof(float[]));
            Assert.AreEqual(new float[] {2, 3, 4}, floats);

            var doubles = svc.testArray(new double[] {1, 2, 3});

            Assert.AreEqual(doubles.GetType(), typeof(double[]));
            Assert.AreEqual(new double[] {2, 3, 4}, doubles);

            var strs = svc.testArray(new[] {"a", "b"});

            Assert.AreEqual(strs.GetType(), typeof(string[]));
            Assert.AreEqual(new[] {"a1", "b1"}, strs);

            var chars = svc.testArray(new[] {'b', 'c'});

            Assert.AreEqual(chars.GetType(), typeof(char[]));
            Assert.AreEqual(new[] {'c', 'd'}, chars);

            var bools = svc.testArray(new[] {true, false, true});

            Assert.AreEqual(bools.GetType(), typeof(bool[]));
            Assert.AreEqual(new[] {false, true, false}, bools);

            // Nulls
            Assert.AreEqual(9, svc.testNull(8));
            Assert.IsNull(svc.testNull(null));

            // params / varargs
            Assert.AreEqual(5, svc.testParams(1, 2, 3, 4, "5"));
            Assert.AreEqual(0, svc.testParams());

            // Overloads
            Assert.AreEqual(3, svc.test(2, "1"));
            Assert.AreEqual(3, svc.test("1", 2));

            // Binary
            Assert.AreEqual(7, svc.testBinarizable(new PlatformComputeBinarizable {Field = 6}).Field);

            DateTime dt = new DateTime(1992, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            Assert.AreEqual(dt, svc.test(dt));
            Assert.AreEqual(dt, svc.testNullTimestamp(dt));
            Assert.IsNull(svc.testNullTimestamp(null));

            var dates = svc.testArray(new DateTime?[] {dt});

            Assert.AreEqual(dates.GetType(), typeof(DateTime?[]));
            Assert.AreEqual(dt, dates[0]);

            Guid guid = Guid.NewGuid();

            Assert.AreEqual(guid, svc.test(guid));
            Assert.AreEqual(guid, svc.testNullUUID(guid));
            Assert.IsNull(svc.testNullUUID(null));

            var guids = svc.testArray(new Guid?[] {guid});

            Assert.AreEqual(guids.GetType(), typeof(Guid?[]));
            Assert.AreEqual(guid, guids[0]);

            DateTime dt1 = new DateTime(1982, 4, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            DateTime dt2 = new DateTime(1991, 10, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            Assert.AreEqual(dt2, svc.testDate(dt1));

            svc.testDateArray(new DateTime?[] {dt1, dt2});

            var cache = Grid1.GetOrCreateCache<int, DateTime>("net-dates");

            cache.Put(1, dt1);
            cache.Put(2, dt2);

            svc.testUTCDateFromCache();

            Assert.AreEqual(dt1, cache.Get(3));
            Assert.AreEqual(dt2, cache.Get(4));

            Assert.AreEqual("value", svc.contextAttribute("attr"));

            const int val = 2;
            Assert.AreEqual(val * val, svc.testInterception(val));

#if NETCOREAPP
            //This Date in Europe/Moscow have offset +4.
            DateTime dt3 = new DateTime(1982, 4, 1, 1, 0, 0, 0, DateTimeKind.Local);
            //This Date in Europe/Moscow have offset +3.
            DateTime dt4 = new DateTime(1982, 3, 31, 22, 0, 0, 0, DateTimeKind.Local);

            cache.Put(5, dt3);
            cache.Put(6, dt4);

            Assert.AreEqual(dt3.ToUniversalTime(), cache.Get(5).ToUniversalTime());
            Assert.AreEqual(dt4.ToUniversalTime(), cache.Get(6).ToUniversalTime());

            svc.testLocalDateFromCache();

            Assert.AreEqual(dt3, cache.Get(7).ToLocalTime());
            Assert.AreEqual(dt4, cache.Get(8).ToLocalTime());

            var now = DateTime.Now;
            cache.Put(9, now);
            Assert.AreEqual(now.ToUniversalTime(), cache.Get(9).ToUniversalTime());
#endif
        }

        /// <summary>
        /// Creates a test caller context.
        /// </summary>
        /// <returns>Caller context.</returns>
        // ReSharper disable once InconsistentNaming
        private IServiceCallContext callContext()
        {
            return new ServiceCallContextBuilder().Set("attr", "value").Build();
        }
    }
}
