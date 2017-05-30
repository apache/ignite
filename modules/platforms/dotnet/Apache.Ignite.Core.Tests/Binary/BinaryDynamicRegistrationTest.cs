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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.Binary
{
    extern alias ExamplesDll;

    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Store;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Tests.Compute;
    using Apache.Ignite.ExamplesDll.Binary;
    using NUnit.Framework;

    using ExamplesAccount = ExamplesDll::Apache.Ignite.ExamplesDll.Binary.Account;

    /// <summary>
    /// Tests the dynamic type registration.
    /// </summary>
    public class BinaryDynamicRegistrationTest
    {
        /// <summary>
        /// Executes before each test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            ClearMarshallerWorkDir();
        }

        /// <summary>
        /// Tests the failed registration.
        /// </summary>
        [Test]
        public void TestFailedRegistration()
        {
            TestFailedRegistration<Foo>(false, false);
            TestFailedRegistration<Bin>(true, false);
            TestFailedRegistration<BinRaw>(true, true);
        }

        /// <summary>
        /// Tests the failed registration, when we write type name after the header.
        /// </summary>
        private static void TestFailedRegistration<T>(bool rawStr, bool rawInt) where T : ITest, new()
        {
            // Disable compact footers for local mode
            var cfg = new BinaryConfiguration {CompactFooter = false};

            // Test in local mode so that MarshallerContext can't propagate type registration.
            var bytes = new Marshaller(cfg).Marshal(new T {Int = 1, Str = "2"});

            var res = new Marshaller(cfg).Unmarshal<T>(bytes);

            Assert.AreEqual(1, res.Int);
            Assert.AreEqual("2", res.Str);

            // Check binary mode
            var bin = new Marshaller(cfg).Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            if (!rawStr)
                Assert.AreEqual("2", bin.GetField<string>("Str"));

            if (!rawInt)
                Assert.AreEqual(1, bin.GetField<int>("Int"));

            res = bin.Deserialize<T>();

            Assert.AreEqual(1, res.Int);
            Assert.AreEqual("2", res.Str);
        }

        /// <summary>
        /// Tests the store with node restart to make sure type names are persisted to disk properly.
        /// </summary>
        [Test]
        public void TestStore()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                // Disable compact footers to test grid restart with persistent store
                // (Because store operates on raw binary objects).
                BinaryConfiguration = new BinaryConfiguration {CompactFooter = false},
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("default")
                    {
                        CacheStoreFactory = new StoreFactory(),
                        ReadThrough = true,
                        WriteThrough = true
                    }
                }
            };

            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                // Put through dynamically started cache
                var dynCache = ignite.CreateCache<int, Foo>(new CacheConfiguration("dynCache")
                {
                    CacheStoreFactory = new StoreFactory(),
                    ReadThrough = true,
                    WriteThrough = true
                });
                dynCache[2] = new Foo { Str = "test2", Int = 3 };

                // Start another server node so that store is initialized there
                using (var ignite2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    IgniteInstanceName = "grid2"
                }))
                {
                    var dynCache2 = ignite2.GetCache<int, Foo>(dynCache.Name);

                    Assert.AreEqual("test2", dynCache2[2].Str);
                    Assert.AreEqual(3, dynCache2[2].Int);
                }
            }

            using (var ignite = Ignition.Start(cfg))
            {
                // Put through statically started cache
                var staticCache = ignite.GetCache<int, Foo>("default");
                staticCache[1] = new Foo {Str = "test", Int = 2};
            }

            using (var ignite = Ignition.Start(cfg))
            {
                var foo = ignite.GetCache<int, Foo>("default")[1];
                var foo2 = ignite.GetCache<int, Foo>("default")[2];

                Assert.AreEqual("test", foo.Str);
                Assert.AreEqual(2, foo.Int);

                Assert.AreEqual("test2", foo2.Str);
                Assert.AreEqual(3, foo2.Int);

                // Client node
                using (var igniteClient = Ignition.Start(new IgniteConfiguration(cfg)
                {
                    ClientMode = true,
                    IgniteInstanceName = "grid2"
                }))
                {
                    var fooClient = igniteClient.GetCache<int, Foo>("default")[1];
                    var fooClient2 = igniteClient.GetCache<int, Foo>("default")[2];

                    Assert.AreEqual("test", fooClient.Str);
                    Assert.AreEqual(2, fooClient.Int);

                    Assert.AreEqual("test2", fooClient2.Str);
                    Assert.AreEqual(3, fooClient2.Int);
                }
            }

            // Delete directory and check that store no longer works
            ClearMarshallerWorkDir();

            using (var ignite = Ignition.Start(cfg))
            {
                var ex = Assert.Throws<BinaryObjectException>(() => ignite.GetCache<int, Foo>("default").Get(1));

                Assert.IsTrue(ex.Message.Contains("Unknown pair"));
            }
        }

        /// <summary>
        /// Tests the store factory property propagation.
        /// </summary>
        [Test]
        public void TestStoreFactory()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("default")
                    {
                        CacheStoreFactory = new StoreFactory {StringProp = "test", IntProp = 9},
                        ReadThrough = true,
                        WriteThrough = true
                    }
                }
            };

            using (Ignition.Start(cfg))
            {
                var storeFactory = StoreFactory.LastInstance;

                Assert.AreEqual("test", storeFactory.StringProp);
                Assert.AreEqual(9, storeFactory.IntProp);
            }
        }

        /// <summary>
        /// Tests the single grid scenario.
        /// </summary>
        [Test]
        public void TestSingleGrid()
        {
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                Test(ignite, ignite);
            }
        }

        /// <summary>
        /// Tests the two grid scenario.
        /// </summary>
        [Test]
        public void TestTwoGrids([Values(false, true)] bool clientMode)
        {
            using (var ignite1 = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    IgniteInstanceName = "grid2",
                    ClientMode = clientMode
                };

                using (var ignite2 = Ignition.Start(cfg))
                {
                    Test(ignite1, ignite2);
                }

                // Test twice to verify double registration.
                using (var ignite2 = Ignition.Start(cfg))
                {
                    Test(ignite1, ignite2);
                }
            }
        }

        /// <summary>
        /// Tests the situation where newly joined node attempts registration of a known type.
        /// </summary>
        [Test]
        public void TestTwoGridsStartStop([Values(false, true)] bool clientMode)
        {
            using (Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    IgniteInstanceName = "grid2",
                    ClientMode = clientMode
                };

                using (var ignite2 = Ignition.Start(cfg))
                {
                    var cache = ignite2.CreateCache<int, Foo>(new CacheConfiguration("foos")
                    {
                        CacheMode = CacheMode.Replicated
                    });

                    cache[1] = new Foo();
                }

                using (var ignite2 = Ignition.Start(cfg))
                {
                    var cache = ignite2.GetCache<int, Foo>("foos");

                    // ignite2 does not know that Foo class is registered in cluster, and attempts to register.
                    cache[2] = new Foo();

                    Assert.AreEqual(0, cache[1].Int);
                    Assert.AreEqual(0, cache[2].Int);
                }
            }
        }

        /// <summary>
        /// Tests interop scenario: Java and .NET exchange an object with the same type id, 
        /// but marshaller cache contains different entries for different platforms for the same id.
        /// </summary>
        [Test]
        public void TestJavaInterop()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = BinaryBasicNameMapper.SimpleNameInstance
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cacheCfg = new CacheConfiguration("default", new QueryEntity(typeof(PlatformComputeBinarizable))
                {
                    Fields = new[] {new QueryField("Field", typeof(int))}
                });

                var cache = ignite.CreateCache<int, object>(cacheCfg);

                // Force dynamic registration for .NET
                cache.Put(1, new PlatformComputeBinarizable {Field = 7});

                // Run Java code that will also perform dynamic registration
                var fromJava = ignite.GetCompute().ExecuteJavaTask<PlatformComputeBinarizable>(ComputeApiTest.EchoTask,
                    ComputeApiTest.EchoTypeBinarizable);

                // Check that objects are compatible
                Assert.AreEqual(1, fromJava.Field);

                // Check that Java can read what .NET has put
                var qryRes = ignite.GetCompute().ExecuteJavaTask<IList>(
                    BinaryCompactFooterInteropTest.PlatformSqlQueryTask, "Field < 10");

                Assert.AreEqual(7, qryRes.OfType<PlatformComputeBinarizable>().Single().Field);
            }
        }

        /// <summary>
        /// Tests that types with same FullName from different assemblies are mapped to each other.
        /// </summary>
        [Test]
        public void TestSameTypeInDifferentAssemblies()
        {
            using (var ignite1 = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var cache1 = ignite1.CreateCache<int, ExamplesAccount>("acc");
                cache1[1] = new ExamplesAccount(1, 2.2m);

                using (var ignite2 = Ignition.Start(TestUtils.GetTestConfiguration(name: "ignite2")))
                {
                    var cache2 = ignite2.GetCache<int, Account>("acc");
                    cache2[2] = new Account {Id = 2, Balance = 3.3m};

                    Assert.AreEqual(1, cache2[1].Id);  // Read ExamplesAccount as Account.
                    Assert.AreEqual(2, cache1[2].Id);  // Read Account as ExamplesAccount.
                }
            }
        }

        /// <summary>
        /// Tests the type registration.
        /// </summary>
        private static void Test(IIgnite ignite1, IIgnite ignite2)
        {
            var cfg = new CacheConfiguration("cache") {CacheMode = CacheMode.Partitioned};

            // Put on one grid.
            var cache1 = ignite1.GetOrCreateCache<int, object>(cfg);
            cache1[1] = new Foo {Int = 1, Str = "1"};
            cache1[2] = ignite1.GetBinary().GetBuilder(typeof (Bar)).SetField("Int", 5).SetField("Str", "s").Build();

            // Get on another grid.
            var cache2 = ignite2.GetOrCreateCache<int, Foo>(cfg);
            var foo = cache2[1];

            Assert.AreEqual(1, foo.Int);
            Assert.AreEqual("1", foo.Str);

            var bar = cache2.WithKeepBinary<int, IBinaryObject>()[2];

            Assert.AreEqual("s", bar.GetField<string>("Str"));
            Assert.AreEqual(5, bar.GetField<int>("Int"));

            var bar0 = bar.Deserialize<Bar>();

            Assert.AreEqual("s", bar0.Str);
            Assert.AreEqual(5, bar0.Int);

            // Test compute.
            var serverNodeCount = ignite1.GetCluster().ForServers().GetNodes().Count;

            var res = ignite1.GetCompute().Broadcast(new CompFn<DateTime>(() => DateTime.Now));
            Assert.AreEqual(serverNodeCount, res.Count);

            // Variable capture.
            var res2 = ignite1.GetCompute().Broadcast(new CompFn<string>(() => bar0.Str));
            Assert.AreEqual(Enumerable.Repeat(bar0.Str, serverNodeCount), res2);
        }

        /// <summary>
        /// Clears the marshaller work dir.
        /// </summary>
        private static void ClearMarshallerWorkDir()
        {
            // Delete all *.classname files within IGNITE_HOME
            var home = IgniteHome.Resolve(null);

            var files = Directory.GetFiles(home, "*.classname*", SearchOption.AllDirectories);

            files.ToList().ForEach(File.Delete);
        }

        private interface ITest
        {
            int Int { get; set; }
            string Str { get; set; }
        }

        private class Foo : ITest
        {
            public int Int { get; set; }
            public string Str { get; set; }
        }

        private class Bar : ITest
        {
            public int Int { get; set; }
            public string Str { get; set; }
        }

        private class Bin : IBinarizable, ITest
        {
            public int Int { get; set; }
            public string Str { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("Int", Int);
                writer.GetRawWriter().WriteString(Str);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Int = reader.ReadInt("Int");
                Str = reader.GetRawReader().ReadString();
            }
        }

        private class BinRaw : IBinarizable, ITest
        {
            public int Int { get; set; }
            public string Str { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                var w = writer.GetRawWriter();

                w.WriteInt(Int);
                w.WriteString(Str);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                var r = reader.GetRawReader();

                Int = r.ReadInt();
                Str = r.ReadString();
            }
        }

        [Serializable]
        private class StoreFactory : IFactory<ICacheStore>
        {
            public string StringProp { get; set; }

            public int IntProp { get; set; }

            public static StoreFactory LastInstance { get; set; }

            public ICacheStore CreateInstance()
            {
                LastInstance = this;
                return new CacheStore();
            }
        }

        private class CacheStore : CacheStoreAdapter<object, object>
        {
            private static readonly Dictionary<object, object>  Dict = new Dictionary<object, object>();

            public override object Load(object key)
            {
                object res;
                return Dict.TryGetValue(key, out res) ? res : null;
            }

            public override void Write(object key, object val)
            {
                Dict[key] = val;
            }

            public override void Delete(object key)
            {
                Dict.Remove(key);
            }
        }

        private class CompFn<T> : IComputeFunc<T>
        {
            private readonly Func<T> _func;

            public CompFn(Func<T> func)
            {
                _func = func;
            }

            public T Invoke()
            {
                return _func();
            }
        }
    }
}

namespace Apache.Ignite.ExamplesDll.Binary
{
    /// <summary>
    /// Copy of Account class in ExamplesDll. Same name and namespace, different assembly.
    /// </summary>
    public class Account
    {
        public int Id { get; set; }

        public decimal Balance { get; set; }
    }
}
