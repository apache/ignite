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

namespace Apache.Ignite.Core.Tests.Client.Services
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Services;
    using Apache.Ignite.Core.Platform;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using Apache.Ignite.Core.Tests.Services;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IServicesClient"/>.
    /// </summary>
    public class ServicesClientTest : ClientTestBase
    {
        /** */
        private const string ServiceName = "SVC_NAME";

        /// <summary>
        /// Initializes a new instance of <see cref="ServicesClientTest"/> class.
        /// </summary>
        public ServicesClientTest() : base(2)
        {
            // No-op.
        }

        /** */
        public ServicesClientTest(bool useBinaryArray) : base(2, useBinaryArray: useBinaryArray)
        {
            // No-op.
        }

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TestTearDown()
        {
            ServerServices.CancelAll();

            TestUtils.AssertHandleRegistryIsEmpty(1000, Ignition.GetAll().ToArray());
        }

        /// <summary>
        /// Tests the basic success path.
        /// </summary>
        [Test]
        public void TestBasicServiceCall()
        {
            var svc = DeployAndGetTestService();

            var res = svc.IntMethod();

            Assert.AreEqual(42, res);
        }

        /// <summary>
        /// Tests that void method can be called and the lack of result is handled correctly.
        ///
        /// - Invoke void method
        /// - Verify invoke count on server
        /// </summary>
        [Test]
        public void TestVoidMethodCall()
        {
            var svc = DeployAndGetTestService();
            var expectedCallCount = TestService.CallCount + 1;

            svc.VoidMethod();

            Assert.AreEqual(expectedCallCount, TestService.CallCount);
        }

        /// <summary>
        /// Tests that objects can be passed to and from service methods.
        /// </summary>
        [Test]
        public void TestObjectMethodCall()
        {
            var svc = DeployAndGetTestService();

            var res = svc.PersonMethod(new Person(1));

            Assert.AreEqual(2, res.Id);
        }

        /// <summary>
        /// Tests that generic method can be called on a service.
        /// </summary>
        [Test]
        [Ignore("IGNITE-13351")]
        public void TestGenericMethodCall()
        {
            var svcName = TestUtils.TestName;
            ServerServices.DeployClusterSingleton(svcName, new TestServiceGenericMethods());

            var svc = Client.GetServices().GetServiceProxy<ITestServiceGenericMethods>(svcName);

            Assert.AreEqual("1", svc.GetGeneric("1"));
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithKeepBinary"/> call causes service invocation results to be
        /// returned in serialized form.
        ///
        /// - Enable binary mode
        /// - Verify that service invocation result is <see cref="IBinaryObject"/>
        /// </summary>
        [Test]
        public void TestClientKeepBinaryReturnsServiceInvocationResultInBinaryMode()
        {
            var svc = DeployAndGetTestService<ITestServiceClient>(s => s.WithKeepBinary());

            var person = Client.GetBinary().ToBinary<IBinaryObject>(new Person(5));
            var res = svc.PersonMethod(person);

            Assert.AreEqual(6, res.GetField<int>("Id"));
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithServerKeepBinary"/> call causes service invocation arguments
        /// to be passed to the service (on server node) in serialized form.
        ///
        /// - Enable server-side binary mode
        /// - Check that server-side service receives <see cref="IBinaryObject"/>, modifies it and returns back
        /// </summary>
        [Test]
        public void TestServerKeepBinaryPassesServerSideArgumentsInBinaryMode()
        {
            var svc = DeployAndGetTestService<ITestServiceClient>(s => s.WithServerKeepBinary());

            var res = svc.PersonMethodBinary(new Person(1));

            Assert.AreEqual(2, res.Id);
        }

        /// <summary>
        /// Tests that <see cref="IServicesClient.WithServerKeepBinary"/> combined with
        /// <see cref="IServicesClient.WithKeepBinary"/> uses binary objects both on client and server sides.
        ///
        /// - Enable server and client binary mode
        /// - Check that both server and client operate on <see cref="IBinaryObject"/> instances
        /// </summary>
        [Test]
        public void TestServerAndClientKeepBinaryPassesBinaryObjectsOnServerAndClient()
        {
            var svc = DeployAndGetTestService(s => s.WithKeepBinary().WithServerKeepBinary());

            var person = Client.GetBinary().ToBinary<IBinaryObject>(new Person(-2));
            var res = svc.PersonMethodBinary(person);

            Assert.AreEqual(-1, res.GetField<int>("Id"));
        }

        /// <summary>
        /// Tests that property getters/setters can be invoked on a remote service.
        /// </summary>
        [Test]
        public void TestPropertyCalls()
        {
            var svc = DeployAndGetTestService();

            // Primitive.
            svc.IntProperty = 99;
            Assert.AreEqual(99, svc.IntProperty);

            // Object.
            svc.PersonProperty= new Person(123);
            Assert.AreEqual(123, svc.PersonProperty.Id);
        }

        /// <summary>
        /// Tests that object array can be passed to and from the remote service.
        /// </summary>
        [Test]
        public void TestObjectArray()
        {
            var svc = DeployAndGetTestService();

            var res = svc.PersonArrayMethod(new[] {new Person(10), new Person(20)});

            Assert.AreEqual(new[] {12, 22}, res.Select(p => p.Id));
        }

        /// <summary>
        /// Tests that object array can be passed to and from the remote service in binary mode.
        /// </summary>
        [Test]
        public void TestObjectArrayBinary()
        {
            var svc = DeployAndGetTestService(s => s.WithKeepBinary().WithServerKeepBinary());

            var persons = new[] {new Person(10), new Person(20)}
                .Select(p => Client.GetBinary().ToBinary<IBinaryObject>(p))
                .ToArray();

            var res = svc.PersonArrayMethodBinary(persons);

            Assert.AreEqual(new[] {12, 22}, res.Select(p => p.GetField<int>("Id")));
        }

        /// <summary>
        /// Tests all primitive and built-in types used as parameters and return values.
        /// </summary>
        [Test]
        public void TestAllArgumentTypes()
        {
            ServerServices.DeployClusterSingleton(ServiceName, new TestServiceDataTypes());
            var svc = Client.GetServices().GetServiceProxy<ITestServiceDataTypes>(ServiceName);

            Assert.AreEqual(2, svc.GetByte(1));
            Assert.AreEqual(new byte[] {3, 4, 5}, svc.GetByteArray(new byte[] {2, 3, 4}));

            Assert.AreEqual(3, svc.GetSbyte(2));
            Assert.AreEqual(new sbyte[] {-4, 6}, svc.GetSbyteArray(new sbyte[] {-5, 5}));

            Assert.AreEqual(3, svc.GetShort(2));
            Assert.AreEqual(new short[] {-4, 6}, svc.GetShortArray(new short[] {-5, 5}));

            Assert.AreEqual(3, svc.GetUShort(2));
            Assert.AreEqual(new ushort[] {1, 6}, svc.GetUShortArray(new ushort[] {0, 5}));

            Assert.AreEqual(3, svc.GetInt(2));
            Assert.AreEqual(new [] {-4, 6}, svc.GetIntArray(new[] {-5, 5}));

            Assert.AreEqual(3, svc.GetUInt(2));
            Assert.AreEqual(new uint[] {1, 6}, svc.GetUIntArray(new uint[] {0, 5}));

            Assert.AreEqual(long.MaxValue - 9, svc.GetLong(long.MaxValue - 10));
            Assert.AreEqual(new [] {long.MinValue + 1, 6}, svc.GetLongArray(new[] {long.MinValue, 5}));

            Assert.AreEqual(ulong.MaxValue - 9, svc.GetULong(ulong.MaxValue - 10));
            Assert.AreEqual(new ulong[] {1, 10}, svc.GetULongArray(new ulong[] {0, 9}));

            Assert.AreEqual('d', svc.GetChar('c'));
            Assert.AreEqual(new[] {'b', 'c'}, svc.GetCharArray(new[]{'a', 'b'}));

            var guid = Guid.NewGuid();
            Assert.AreEqual(guid, svc.GetGuid(guid));
            Assert.AreEqual(new[] {guid, Guid.Empty}, svc.GetGuidArray(new[] {guid, Guid.Empty}));

            var dt = DateTime.Now;
            Assert.AreEqual(dt.AddDays(1), svc.GetDateTime(dt));
            Assert.AreEqual(new[] {dt.AddDays(1), dt.AddDays(2)}, svc.GetDateTimeArray(new[] {dt, dt.AddDays(1)}));
            Assert.AreEqual(new List<DateTime> {dt.AddDays(1), dt.AddDays(2)},
                svc.GetDateTimeList(new[] {dt, dt.AddDays(1)}.ToList()));

            var ts = TimeSpan.FromSeconds(25);
            var minuteTs = TimeSpan.FromMinutes(1);
            Assert.AreEqual(ts.Add(minuteTs), svc.GetTimeSpan(ts));
            Assert.AreEqual(new[] {ts.Add(minuteTs), minuteTs}, svc.GetTimeSpanArray(new[] {ts, TimeSpan.Zero}));

            Assert.AreEqual(true, svc.GetBool(false));
            Assert.AreEqual(new[] {true, false}, svc.GetBoolArray(new[] {false, true}));

            Assert.AreEqual(1.5f, svc.GetFloat(0.5f));
            Assert.AreEqual(new[] {-0.5f, 1.1f}, svc.GetFloatArray(new[] {-1.5f, 0.1f}));

            Assert.AreEqual(1.5d, svc.GetDouble(0.5d));
            Assert.AreEqual(new[] {-7.02d, 1.1d}, svc.GetDoubleArray(new[] {-8.02d, 0.1d}));

            Assert.AreEqual(1.666m, svc.GetDecimal(0.666m));
            Assert.AreEqual(new[] {-7.66m, 1.33m}, svc.GetDecimalArray(new[] {-8.66m, 0.33m}));

            Assert.AreEqual("foo", svc.GetString("FOO"));
            Assert.AreEqual(new[]{"foo", "bar"}, svc.GetStringArray(new[]{"FoO", "bAr"}));
        }

        /// <summary>
        /// Tests that overloaded methods are resolved correctly.
        ///
        /// - Invoke multiple overloads of the same method
        /// - Check that correct overload is invoked based on the return value
        /// </summary>
        [Test]
        public void TestOverloadResolution()
        {
            var svcName = TestUtils.TestName;
            ServerServices.DeployClusterSingleton(svcName, new TestServiceOverloads());
            var svc = Client.GetServices().GetServiceProxy<ITestServiceOverloads>(svcName);

            Assert.AreEqual(true, svc.Foo());
            Assert.AreEqual(1, svc.Foo(default(int)));
            Assert.AreEqual(3, svc.Foo(default(byte)));
            Assert.AreEqual(4, svc.Foo(default(short)));
            Assert.AreEqual(6, svc.Foo(new Person()));
            Assert.AreEqual(8, svc.Foo(new[] {1}));
            Assert.AreEqual(9, svc.Foo(new[] {new object()}));

            // Unsigned types are not preserved by the binary protocol and resolve to signed counterparts.
            Assert.AreEqual(1, svc.Foo(default(uint)));
            Assert.AreEqual(4, svc.Foo(default(ushort)));

            if (!UseBinaryArray)
            {
                // Array types are not distinguished.
                Assert.AreEqual(9, svc.Foo(new[] {new Person(0)}));
            }
            else
            {
                Assert.AreEqual(9, svc.Foo(new object[] {new Person(0)}));
                Assert.AreEqual(10, svc.Foo(new[] {new Person(0)}));
                Assert.AreEqual(10, svc.Foo(new[] {new Person(0)}));
            }
        }

        /// <summary>
        /// Tests that thin client can call Java services.
        /// </summary>
        [Test]
        public void TestJavaServiceCall()
        {
            TestUtils.DeployJavaService(Ignition.GetIgnite());
            var svc = Client.GetServices().GetServiceProxy<IJavaService>(TestUtils.JavaServiceName);
            var binSvc = Client.GetServices()
                .WithKeepBinary()
                .WithServerKeepBinary()
                .GetServiceProxy<IJavaService>(TestUtils.JavaServiceName);

            Assert.IsTrue(svc.isInitialized());
            Assert.IsTrue(svc.isExecuted());
            Assert.IsFalse(svc.isCancelled());

            // Primitives.
            Assert.AreEqual(4, svc.test((byte) 3));
            Assert.AreEqual(5, svc.test((short) 4));
            Assert.AreEqual(6, svc.test(5));
            Assert.AreEqual(6, svc.test((long) 5));
            Assert.AreEqual(3.8f, svc.test(2.3f));
            Assert.AreEqual(5.8, svc.test(3.3));
            Assert.IsFalse(svc.test(true));
            Assert.AreEqual('b', svc.test('a'));
            Assert.AreEqual("Foo!", svc.test("Foo"));

            // Nullables (Java wrapper types).
            Assert.AreEqual(4, svc.testWrapper(3));
            Assert.AreEqual(5, svc.testWrapper((short?) 4));
            Assert.AreEqual(6, svc.testWrapper((int?)5));
            Assert.AreEqual(6, svc.testWrapper((long?) 5));
            Assert.AreEqual(3.8f, svc.testWrapper(2.3f));
            Assert.AreEqual(5.8, svc.testWrapper(3.3));
            Assert.AreEqual(false, svc.testWrapper(true));
            Assert.AreEqual('b', svc.testWrapper('a'));

            // Arrays.
            Assert.AreEqual(new byte[] {2, 3, 4}, svc.testArray(new byte[] {1, 2, 3}));
            Assert.AreEqual(new short[] {2, 3, 4}, svc.testArray(new short[] {1, 2, 3}));
            Assert.AreEqual(new[] {2, 3, 4}, svc.testArray(new[] {1, 2, 3}));
            Assert.AreEqual(new long[] {2, 3, 4}, svc.testArray(new long[] {1, 2, 3}));
            Assert.AreEqual(new float[] {2, 3, 4}, svc.testArray(new float[] {1, 2, 3}));
            Assert.AreEqual(new double[] {2, 3, 4}, svc.testArray(new double[] {1, 2, 3}));
            Assert.AreEqual(new[] {"a1", "b1"}, svc.testArray(new [] {"a", "b"}));
            Assert.AreEqual(new[] {'c', 'd'}, svc.testArray(new[] {'b', 'c'}));
            Assert.AreEqual(new[] {false, true, false}, svc.testArray(new[] {true, false, true}));

            // Nulls.
            Assert.AreEqual(9, svc.testNull(8));
            Assert.IsNull(svc.testNull(null));

            // params / varargs.
            Assert.AreEqual(5, svc.testParams(1, 2, 3, 4, "5"));
            Assert.AreEqual(0, svc.testParams());

            // Overloads.
            Assert.AreEqual(3, svc.test(2, "1"));
            Assert.AreEqual(3, svc.test("1", 2));

            // Dates & Timestamps: not supported in Thin Client Services.
            var ex = Assert.Throws<IgniteClientException>(() => svc.test(DateTime.UtcNow));
            StringAssert.StartsWith("Failed to resolve .NET class 'System.DateTime' in Java", ex.Message);

            // Guid.
            var guid = Guid.NewGuid();

            Assert.AreEqual(guid, svc.test(guid));
            Assert.AreEqual(guid, svc.testNullUUID(guid));
            Assert.IsNull(svc.testNullUUID(null));
            Assert.AreEqual(guid, svc.testArray(new Guid?[] {guid})[0]);

            // Binary object.
            Assert.AreEqual(15,
                binSvc.testBinaryObject(
                    Client.GetBinary().ToBinary<IBinaryObject>(new PlatformComputeBinarizable {Field = 6}))
                    .GetField<int>("Field"));

            // Binary object array.
            var arr  = new[] {10, 11, 12}.Select(
                x => new PlatformComputeBinarizable {Field = x}).ToArray();

            var binArr = arr.Select(Client.GetBinary().ToBinary<IBinaryObject>).ToArray();

            Assert.AreEqual(new[] {11, 12, 13}, binSvc.testBinaryObjectArray(binArr)
                .Select(x => x.GetField<int>("Field")));
        }

        /// <summary>
        /// Tests that specifying custom cluster group causes service calls to be routed to selected servers.
        ///
        /// - Deploy the service on every server node with DeployNodeSingleton
        /// - For every server node:
        /// - Get a cluster group of a single node
        /// - Invoke service method that returns local node id
        /// - Check that specified server has been used to invoke the service
        /// </summary>
        [Test]
        public void TestServicesWithCustomClusterGroupInvokeOnSpecifiedNodes()
        {
            ServerServices.DeployNodeSingleton(ServiceName, new TestService());

            foreach (var ignite in Ignition.GetAll())
            {
                var node = ignite.GetCluster().GetLocalNode();
                var clusterGroup = Client.GetCluster().ForPredicate(n => n.Id == node.Id);
                var svc = clusterGroup.GetServices();

                Assert.AreSame(clusterGroup, svc.ClusterGroup);
                Assert.AreEqual(node.Id, clusterGroup.GetNodes().Single().Id);

                var actualNodeId = svc.GetServiceProxy<ITestService>(ServiceName).GetNodeId();
                Assert.AreEqual(node.Id, actualNodeId);
            }
        }

        /// <summary>
        /// Tests that empty cluster group causes exception on service call.
        ///
        /// - Create an empty cluster group
        /// - Get services over that group
        /// - Execute service method, verify exception
        /// </summary>
        [Test]
        public void TestEmptyClusterGroupThrowsError()
        {
            ServerServices.DeployNodeSingleton(ServiceName, new TestService());

            var clusterGroup = Client.GetCluster().ForPredicate(_ => false);
            var svc = clusterGroup.GetServices().GetServiceProxy<ITestService>(ServiceName);

            var ex = Assert.Throws<IgniteClientException>(() => svc.VoidMethod());
            Assert.AreEqual("Cluster group is empty", ex.Message);
        }

        /// <summary>
        /// Tests that a custom cluster group that does not have any server nodes with specified service produces
        /// a correct exception.
        ///
        /// - Deploy the service to node X
        /// - Create client cluster group with node Y
        /// - Call service, verify exception
        /// </summary>
        [Test]
        public void TestClusterGroupWithoutMatchingServiceNodesThrowsError()
        {
            var ignite = Ignition.GetIgnite();
            var node = ignite.GetCluster().GetLocalNode();

            ignite.GetCluster()
                .ForNodes(node)
                .GetServices()
                .DeployClusterSingleton(ServiceName, new TestService());

            var svc = Client.GetCluster()
                .ForPredicate(n => n.Id != node.Id)
                .GetServices()
                .GetServiceProxy<ITestService>(ServiceName);

            var ex = Assert.Throws<IgniteClientException>(() => svc.VoidMethod());
            Assert.AreEqual("Failed to find deployed service: " + ServiceName, ex.Message);
        }

        /// <summary>
        /// Tests that lingering service calls cause timeout exception when WithTimeout is used.
        ///
        /// - Deploy the service
        /// - Get a service proxy with a timeout
        /// - Execute a method that takes a long time, verify that timeout setting takes effect
        /// </summary>
        [Test]
        [Ignore("IGNITE-13360")]
        public void TestTimeout()
        {
            var svc = DeployAndGetTestService();

            var ex = Assert.Throws<IgniteClientException>(() => svc.Sleep(TimeSpan.FromSeconds(3)));

            Assert.AreEqual("timed out", ex.Message);
        }

        /// <summary>
        /// Tests that lingering service calls cause timeout exception when WithTimeout is used.
        ///
        /// - Deploy the service
        /// - Get a service proxy with a timeout
        /// - Execute a method that takes a long time, verify that timeout setting takes effect
        /// </summary>
        [Test]
        [Ignore("IGNITE-13360")]
        public void TestJavaServiceTimeout()
        {
            TestUtils.DeployJavaService(Ignition.GetIgnite());

            var svc = Client.GetServices().GetServiceProxy<IJavaService>(TestUtils.JavaServiceName);

            var ex = Assert.Throws<IgniteClientException>(() => svc.sleep(2000));

            Assert.AreEqual("timed out", ex.Message);
        }

        /// <summary>
        /// Tests that exception in service is propagated to the client and service is still operational.
        /// </summary>
        [Test]
        public void TestExceptionInServiceIsPropagatedToClient()
        {
            var svc = DeployAndGetTestService();

            var ex = Assert.Throws<IgniteClientException>(() => svc.ExceptionalMethod());

            Assert.AreEqual("Failed to invoke platform service, see server logs for details", ex.Message);
        }

        /// <summary>
        /// Tests that invoking a service that does not exist causes a correct exception.
        /// </summary>
        [Test]
        public void TestNonExistentServiceNameCausesClientException()
        {
            var ex = Assert.Throws<IgniteClientException>(() => Client.GetServices().GetServiceProxy<ITestService>(ServiceName));
            Assert.AreEqual(ClientStatusCode.Fail, ex.StatusCode);
        }

        /// <summary>
        /// Tests async method calls.
        /// </summary>
        [Test]
        [Ignore("IGNITE-13343")]
        public void TestAsyncServiceCalls()
        {
            var svc = DeployAndGetTestService();

            var task = svc.AsyncMethod();
            task.Wait();

            Assert.AreEqual(1, task.Result);
        }
        
        /// <summary>
        /// Tests custom caller context.
        /// </summary>
        [Test]
        public void TestServiceCallContext()
        {
            string attrName = "attr";
            string binAttrName = "binAttr";
            string attrValue = "value";
            byte[] binAttrValue = Encoding.UTF8.GetBytes(attrValue);
            
            IServiceCallContext callCtx = new ServiceCallContextBuilder()
                .Set(attrName, attrValue)
                .Set(binAttrName, binAttrValue)
                .Build();

            var svc = DeployAndGetTestService<ITestService>(null, callCtx);

            Assert.AreEqual(attrValue, svc.ContextAttribute(attrName));
            Assert.AreEqual(binAttrValue, svc.ContextBinaryAttribute(binAttrName));

            Assert.Throws<ArgumentException>(() =>
                DeployAndGetTestService<ITestService>(null, new CustomServiceCallContext()));
        }           

        [Test]
        public void TestGetServiceDescriptors()
        {
            DeployAndGetTestService();

            var svcs = Client.GetServices().GetServiceDescriptors();

            Assert.AreEqual(1, svcs.Count);

            var svc = svcs.First();

            Assert.AreEqual(ServiceName, svc.Name);
            Assert.AreEqual(
                "org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetServiceImpl",
                svc.ServiceClass
            );
            Assert.AreEqual(1, svc.TotalCount);
            Assert.AreEqual(1, svc.MaxPerNodeCount);
            Assert.IsNull(svc.CacheName);
            Assert.AreEqual(Ignition.GetIgnite().GetCluster().GetLocalNode().Id, svc.OriginNodeId);
            Assert.AreEqual(PlatformType.DotNet, svc.PlatformType);

            var svc1 = Client.GetServices().GetServiceDescriptor(ServiceName);

            Assert.AreEqual(svc.Name, svc1.Name);
            Assert.AreEqual(svc.ServiceClass, svc1.ServiceClass);
            Assert.AreEqual(svc.TotalCount, svc1.TotalCount);
            Assert.AreEqual(svc.MaxPerNodeCount, svc1.MaxPerNodeCount);
            Assert.AreEqual(svc.CacheName, svc1.CacheName);
            Assert.AreEqual(svc.OriginNodeId, svc1.OriginNodeId);
            Assert.AreEqual(svc.PlatformType, svc1.PlatformType);
        }

        /// <summary>
        /// Deploys test service and returns client-side proxy.
        /// </summary>
        private ITestService DeployAndGetTestService(Func<IServicesClient, IServicesClient> transform = null)
        {
            return DeployAndGetTestService<ITestService>(transform);
        }

        /// <summary>
        /// Deploys test service and returns client-side proxy.
        /// </summary>
        private T DeployAndGetTestService<T>(Func<IServicesClient, IServicesClient> transform = null, 
            IServiceCallContext callCtx = null) where T : class
        {
            ServerServices.DeployClusterSingleton(ServiceName, new TestService());

            var services = Client.GetServices();

            if (transform != null)
            {
                services = transform(services);
            }

            return services.GetServiceProxy<T>(ServiceName, callCtx);
        }

        /// <summary>
        /// Gets server-side Services.
        /// </summary>
        private static IServices ServerServices
        {
            get { return Ignition.GetIgnite().GetServices(); }
        }
        
        /// <summary>
        /// Custom implementation of the service call context.
        /// </summary>
        private class CustomServiceCallContext : IServiceCallContext
        {
            /** <inheritdoc /> */
            public string GetAttribute(string name)
            {
                return null;
            }

            /** <inheritdoc /> */
            public byte[] GetBinaryAttribute(string name)
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Tests for <see cref="IServicesClient"/>.
    /// </summary>
    public class ServicesClientTestBinaryArrays : ServicesClientTest
    {
        /** */
        public ServicesClientTestBinaryArrays() : base(true)
        {
            // No-op.
        }
    }
}
