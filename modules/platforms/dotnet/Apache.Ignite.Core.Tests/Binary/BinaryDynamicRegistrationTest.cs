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
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests the dynamic type registration.
    /// </summary>
    public class BinaryDynamicRegistrationTest
    {
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
            // Make sure marshaller dir is empty
            var marshDir = Path.Combine(GetWorkDir(), "marshaller");

            if (Directory.Exists(marshDir))
                Directory.Delete(marshDir, true);

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration {CompactFooter = false},
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        CacheStoreFactory = new StoreFactory(),
                        ReadThrough = true,
                        WriteThrough = true
                    }
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                ignite.GetCache<int, Foo>(null)[1] = new Foo {Str = "test", Int = 2};
            }

            using (var ignite = Ignition.Start(cfg))
            {
                var foo = ignite.GetCache<int, Foo>(null)[1];

                Assert.AreEqual("test", foo.Str);
                Assert.AreEqual(2, foo.Int);
            }

            // Delete directory and check that store no longer works
            Directory.Delete(marshDir, true);

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.Throws<BinaryObjectException>(() => ignite.GetCache<int, Foo>(null).Get(1));
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
        public void TestTwoGrids()
        {
            using (var ignite1 = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                using (var ignite2 = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    GridName = "grid2"
                }))
                {
                    Test(ignite1, ignite2);
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
            using (var ignite = Ignition.Start(TestUtils.GetTestConfiguration()))
            {
                var cacheCfg = new CacheConfiguration(null, new QueryEntity(typeof(PlatformComputeBinarizable))
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
        /// Tests the type registration.
        /// </summary>
        private static void Test(IIgnite ignite1, IIgnite ignite2)
        {
            const string cacheName = "cache";

            // Put on one grid
            var cache1 = ignite1.CreateCache<int, object>(cacheName);
            cache1[1] = new Foo {Int = 1, Str = "1"};
            cache1[2] = ignite1.GetBinary().GetBuilder(typeof (Bar)).SetField("Int", 5).SetField("Str", "s").Build();

            // Get on another grid
            var cache2 = ignite2.GetCache<int, Foo>(cacheName);
            var foo = cache2[1];

            Assert.AreEqual(1, foo.Int);
            Assert.AreEqual("1", foo.Str);

            var bar = cache2.WithKeepBinary<int, IBinaryObject>()[2];

            Assert.AreEqual("s", bar.GetField<string>("Str"));
            Assert.AreEqual(5, bar.GetField<int>("Int"));

            var bar0 = bar.Deserialize<Bar>();

            Assert.AreEqual("s", bar0.Str);
            Assert.AreEqual(5, bar0.Int);

            // Test compute
            var res = ignite1.GetCompute().Broadcast(new CompFn<DateTime>(() => DateTime.Now));
            Assert.AreEqual(ignite1.GetCluster().GetNodes().Count, res.Count);
        }

        /// <summary>
        /// Gets the work dir.
        /// </summary>
        private static string GetWorkDir()
        {
            var dir = typeof(BinaryDynamicRegistrationTest).Assembly.Location;

            // Java resolves the work dir to the project folder
            while (!File.Exists(Path.Combine(dir, "Apache.Ignite.Core.Tests.csproj")))
                dir = Directory.GetParent(dir).FullName;

            return Path.Combine(dir, "work");
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

        private class StoreFactory : IFactory<ICacheStore>
        {
            public ICacheStore CreateInstance()
            {
                return new CacheStore();
            }
        }

        private class CacheStore : CacheStoreAdapter
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
