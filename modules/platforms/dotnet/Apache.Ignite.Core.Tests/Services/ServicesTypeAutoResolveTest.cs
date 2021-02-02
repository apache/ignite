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
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;
    using Apache.Ignite.Platform.Model;

    /// <summary>
    /// Tests checks ability to execute service method without explicit registration of parameter type.
    /// </summary>
    public class ServicesTypeAutoResolveTest
    {
        /** */
        private readonly bool _useBinaryArray;

        /** Platform service name. */
        const string PlatformSvcName = "PlatformTestService";

        /** Java service name. */
        private string _javaSvcName;

        /** */
        protected internal static readonly Employee[] Emps = new[]
        {
            new Employee {Fio = "Sarah Connor", Salary = 1},
            new Employee {Fio = "John Connor", Salary = 2}
        };

        /** */
        protected internal static readonly Parameter[] Param = new[]
        {
            new Parameter()
                {Id = 1, Values = new[] {new ParamValue() {Id = 1, Val = 42}, new ParamValue() {Id = 2, Val = 43}}},
            new Parameter()
                {Id = 2, Values = new[] {new ParamValue() {Id = 3, Val = 44}, new ParamValue() {Id = 4, Val = 45}}}
        };

        /** */
        private IIgnite _grid1;

        /** */
        private IIgnite _client;

        /** */
        private IIgniteClient _thinClient;

        /** */
        public ServicesTypeAutoResolveTest()
        {
            // No-op.
        }

        /** */
        public ServicesTypeAutoResolveTest(bool useBinaryArray)
        {
            _useBinaryArray = useBinaryArray;
        }

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

            _grid1.GetServices().DeployClusterSingleton(PlatformSvcName, new PlatformTestService());
            _javaSvcName = TestUtils.DeployJavaService(_grid1);
        }

        /// <summary>
        /// Executes after each test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            try
            {
                _grid1.GetServices().Cancel(PlatformSvcName);
                _grid1.GetServices().Cancel(_javaSvcName);

                _grid1.GetServices();

                TestUtils.AssertHandleRegistryIsEmpty(1000, _grid1);
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
        /// Tests .Net service invocation on local node.
        /// </summary>
        [Test]
        public void TestPlatformServiceLocal()
        {
            DoTestService(_grid1.GetServices().GetServiceProxy<IJavaService>(PlatformSvcName), true);
        }

        /// <summary>
        /// Tests .Net service invocation on remote node.
        /// </summary>
        [Test]
        public void TestPlatformServiceRemote()
        {
            DoTestService(_client.GetServices().GetServiceProxy<IJavaService>(PlatformSvcName), true);
        }

        /// <summary>
        /// Tests Java service invocation with dynamic proxy.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestJavaServiceDynamicProxy()
        {
            DoTestService(new JavaServiceDynamicProxy(_grid1.GetServices().GetDynamicServiceProxy(_javaSvcName, true)));
        }

        /// <summary>
        /// Tests Java service invocation on local.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestJavaServiceLocal()
        {
            DoTestService(_grid1.GetServices().GetServiceProxy<IJavaService>(_javaSvcName, false));
        }

        /// <summary>
        /// Tests Java service invocation on remote node..
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestJavaServiceRemote()
        {
            DoTestService(_client.GetServices().GetServiceProxy<IJavaService>(_javaSvcName, false));
        }

        /// <summary>
        /// Tests Java service invocation.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestJavaServiceThinClient()
        {
            DoTestService(_thinClient.GetServices().GetServiceProxy<IJavaService>(_javaSvcName));
        }

        /// <summary>
        /// Tests Platform service invocation.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestPlatformServiceThinClient()
        {
            DoTestService(_thinClient.GetServices().GetServiceProxy<IJavaService>(PlatformSvcName), true);
        }

        /// <summary>
        /// Tests service invocation.
        /// </summary>
        private void DoTestService(IJavaService svc, bool isPlatform = false)
        {
            Assert.IsNull(svc.testDepartments(null));

            var arr = new[] { "HR", "IT" }.Select(x => new Department() { Name = x }).ToList();

            ICollection deps = svc.testDepartments(arr);

            Assert.NotNull(deps);
            Assert.AreEqual(1, deps.Count);
            Assert.AreEqual("Executive", deps.OfType<Department>().Select(d => d.Name).ToArray()[0]);

            Assert.IsNull(svc.testAddress(null));

            Address addr = svc.testAddress(new Address { Zip = "000", Addr = "Moscow" });

            Assert.AreEqual("127000", addr.Zip);
            Assert.AreEqual("Moscow Akademika Koroleva 12", addr.Addr);

            if (_useBinaryArray)
            {
                Assert.AreEqual(42, svc.testOverload(2, Emps));
                Assert.AreEqual(43, svc.testOverload(2, Param));
                Assert.AreEqual(3, svc.testOverload(1, 2));
                Assert.AreEqual(5, svc.testOverload(3, 2));
            }

            Assert.IsNull(svc.testEmployees(null));

            var emps = svc.testEmployees(Emps);

            if (!isPlatform || _useBinaryArray)
                Assert.AreEqual(typeof(Employee[]), emps.GetType());

            Assert.NotNull(emps);
            Assert.AreEqual(1, emps.Length);

            Assert.AreEqual("Kyle Reese", emps[0].Fio);
            Assert.AreEqual(3, emps[0].Salary);

            Assert.IsNull(svc.testMap(null));

            var map = new Dictionary<Key, Value>();

            map.Add(new Key() { Id = 1 }, new Value() { Val = "value1" });
            map.Add(new Key() { Id = 2 }, new Value() { Val = "value2" });

            var res = svc.testMap(map);

            Assert.NotNull(res);
            Assert.AreEqual(1, res.Count);
            Assert.AreEqual("value3", ((Value)res[new Key() { Id = 3 }]).Val);

            var accs = svc.testAccounts();

            if (!isPlatform || _useBinaryArray)
                Assert.AreEqual(typeof(Account[]), accs.GetType());

            Assert.NotNull(accs);
            Assert.AreEqual(2, accs.Length);
            Assert.AreEqual("123", accs[0].Id);
            Assert.AreEqual("321", accs[1].Id);
            Assert.AreEqual(42, accs[0].Amount);
            Assert.AreEqual(0, accs[1].Amount);

            var users = svc.testUsers();

            if (!isPlatform || _useBinaryArray)
                Assert.AreEqual(typeof(User[]), users.GetType());

            Assert.NotNull(users);
            Assert.AreEqual(2, users.Length);
            Assert.AreEqual(1, users[0].Id);
            Assert.AreEqual(ACL.ALLOW, users[0].Acl);
            Assert.AreEqual("admin", users[0].Role.Name);
            Assert.AreEqual(AccessLevel.SUPER, users[0].Role.AccessLevel);
            Assert.AreEqual(2, users[1].Id);
            Assert.AreEqual(ACL.DENY, users[1].Acl);
            Assert.AreEqual("user", users[1].Role.Name);
            Assert.AreEqual(AccessLevel.USER, users[1].Role.AccessLevel);

            var users2 = svc.testRoundtrip(users);

            Assert.NotNull(users2);

            if (_useBinaryArray)
                Assert.AreEqual(typeof(User[]), users2.GetType());
        }

        /// <summary>
        /// Tests Java service invocation.
        /// Types should be resolved implicitly.
        /// </summary>
        [Test]
        public void TestMessagingJavaService()
        {
            // Deploy Java service.
            var javaSvcName = TestUtils.DeployJavaService(_grid1);

            var svc = _grid1.GetServices().GetServiceProxy<IJavaService>(javaSvcName, true);

            var msgng = _grid1.GetMessaging();

            var rcvd = new List<V5>();

            var lsnr = new MessageListener<V5>((guid, v) =>
            {
                rcvd.Add(v);

                return true;
            });

            msgng.LocalListen(lsnr, "test-topic");

            svc.testSendMessage();

            TestUtils.WaitForTrueCondition(() => rcvd.Count == 3, timeout: 2500);

            Assert.IsNotNull(rcvd.Find(v => v.Name == "1"));
            Assert.IsNotNull(rcvd.Find(v => v.Name == "2"));
            Assert.IsNotNull(rcvd.Find(v => v.Name == "3"));

            msgng.StopLocalListen(lsnr, "test-topic");

            svc.startReceiveMessage();

            msgng.Send(new V6 {Name = "Sarah Connor"}, "test-topic-2");
            msgng.Send(new V6 {Name = "John Connor"}, "test-topic-2");
            msgng.Send(new V6 {Name = "Kyle Reese"}, "test-topic-2");

            msgng.SendAll(new[]
            {
                new V7 {Name = "V7-1"},
                new V7 {Name = "V7-2"},
                new V7 {Name = "V7-3"}
            }, "test-topic-3");

            msgng.SendOrdered(new V8 {Name = "V8"}, "test-topic-4");
            msgng.SendOrdered(new V8 {Name = "V9"}, "test-topic-4");
            msgng.SendOrdered(new V8 {Name = "V10"}, "test-topic-4");

            Assert.IsTrue(svc.testMessagesReceived());
        }

        /// <summary>
        /// Starts the grids.
        /// </summary>
        private void StartGrids()
        {
            if (_grid1 != null)
                return;

            var path = Path.Combine("Config", "Compute", "compute-grid");

            var cfg = GetConfiguration(path + "1.xml");

            _grid1 = Ignition.Start(cfg);

            cfg.ClientMode = true;
            cfg.IgniteInstanceName = "client";
            cfg.WorkDirectory = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),
                "client_work");

            _client = Ignition.Start(cfg);
            _thinClient = Ignition.StartClient(GetClientConfiguration());
        }

        /// <summary>
        /// Stops the grids.
        /// </summary>
        private void StopGrids()
        {
            _grid1 = null;

            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets the Ignite configuration.
        /// </summary>
        private IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            springConfigUrl = Compute.ComputeApiTestFullFooter.ReplaceFooterSetting(springConfigUrl);

            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = springConfigUrl,
                BinaryConfiguration = BinaryConfiguration(),
                LifecycleHandlers = _useBinaryArray ? new[] { new SetUseBinaryArray() } : null
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
                BinaryConfiguration = BinaryConfiguration()
            };
        }

        /** */
        private BinaryConfiguration BinaryConfiguration()
        {
            return new BinaryConfiguration
            {
                NameMapper = new BinaryBasicNameMapper { NamespacePrefix = "org.", NamespaceToLower = true }
            };
        }
    }

    /// <summary>
    /// Tests checks ability to execute service method without explicit registration of parameter type.
    /// </summary>
    public class ServicesTypeAutoResolveTestBinaryArrays : ServicesTypeAutoResolveTest
    {
        /** */
        public ServicesTypeAutoResolveTestBinaryArrays() : base(true)
        {
            // No-op.
        }
    }
}
