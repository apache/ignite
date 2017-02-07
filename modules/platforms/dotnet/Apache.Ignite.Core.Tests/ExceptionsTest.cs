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

#pragma warning disable 618
namespace Apache.Ignite.Core.Tests 
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests grid exceptions propagation.
    /// </summary>
    public class ExceptionsTest
    {
        /** */
        private const string ExceptionTask = "org.apache.ignite.platform.PlatformExceptionTask";

        /// <summary>
        /// Before test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            TestUtils.KillProcesses();
        }

        /// <summary>
        /// After test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests exceptions.
        /// </summary>
        [Test]
        public void TestExceptions()
        {
            var grid = StartGrid();

            Assert.Throws<ArgumentException>(() => grid.GetCache<object, object>("invalidCacheName"));

            var e = Assert.Throws<ClusterGroupEmptyException>(() => grid.GetCluster().ForRemotes().GetMetrics());

            Assert.IsNotNull(e.InnerException);

            Assert.IsTrue(e.InnerException.Message.StartsWith(
                "class org.apache.ignite.cluster.ClusterGroupEmptyException: Cluster group is empty."));

            // Check all exceptions mapping.
            var comp = grid.GetCompute();

            CheckException<BinaryObjectException>(comp, "BinaryObjectException");
            CheckException<IgniteException>(comp, "IgniteException");
            CheckException<BinaryObjectException>(comp, "BinaryObjectException");
            CheckException<ClusterTopologyException>(comp, "ClusterTopologyException");
            CheckException<ComputeExecutionRejectedException>(comp, "ComputeExecutionRejectedException");
            CheckException<ComputeJobFailoverException>(comp, "ComputeJobFailoverException");
            CheckException<ComputeTaskCancelledException>(comp, "ComputeTaskCancelledException");
            CheckException<ComputeTaskTimeoutException>(comp, "ComputeTaskTimeoutException");
            CheckException<ComputeUserUndeclaredException>(comp, "ComputeUserUndeclaredException");
            CheckException<TransactionOptimisticException>(comp, "TransactionOptimisticException");
            CheckException<TransactionTimeoutException>(comp, "TransactionTimeoutException");
            CheckException<TransactionRollbackException>(comp, "TransactionRollbackException");
            CheckException<TransactionHeuristicException>(comp, "TransactionHeuristicException");
            CheckException<TransactionDeadlockException>(comp, "TransactionDeadlockException");
            CheckException<IgniteFutureCancelledException>(comp, "IgniteFutureCancelledException");

            // Check stopped grid.
            grid.Dispose();

            Assert.Throws<InvalidOperationException>(() => grid.GetCache<object, object>("cache1"));
        }

        /// <summary>
        /// Checks the exception.
        /// </summary>
        private static void CheckException<T>(ICompute comp, string name) where T : Exception
        {
            var ex = Assert.Throws<T>(() => comp.ExecuteJavaTask<string>(ExceptionTask, name));

            var javaEx = ex.InnerException as JavaException;

            Assert.IsNotNull(javaEx);
            Assert.IsTrue(javaEx.Message.Contains("at " + ExceptionTask));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPartialUpdateException()
        {
            // Primitive type
            TestPartialUpdateException(false, (x, g) => x);

            // User type
            TestPartialUpdateException(false, (x, g) => new BinarizableEntry(x));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation in binary mode.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPartialUpdateExceptionBinarizable()
        {
            // User type
            TestPartialUpdateException(false, (x, g) => g.GetBinary().ToBinary<IBinaryObject>(new BinarizableEntry(x)));
        }

        /// <summary>
        /// Tests CachePartialUpdateException serialization.
        /// </summary>
        [Test]
        public void TestPartialUpdateExceptionSerialization()
        {
            // Inner exception
            TestPartialUpdateExceptionSerialization(new CachePartialUpdateException("Msg",
                new IgniteException("Inner msg")));

            // Primitive keys
            TestPartialUpdateExceptionSerialization(new CachePartialUpdateException("Msg", new object[] {1, 2, 3}));

            // User type keys
            TestPartialUpdateExceptionSerialization(new CachePartialUpdateException("Msg",
                new object[]
                {
                    new SerializableEntry(1),
                    new SerializableEntry(2),
                    new SerializableEntry(3)
                }));
        }

        /// <summary>
        /// Tests that all exceptions have mandatory constructors and are serializable.
        /// </summary>
        [Test]
        public void TestAllExceptionsConstructors()
        {
            var types = typeof(IIgnite).Assembly.GetTypes().Where(x => x.IsSubclassOf(typeof(Exception)));

            foreach (var type in types)
            {
                Assert.IsTrue(type.IsSerializable, "Exception is not serializable: " + type);

                // Default ctor.
                var defCtor = type.GetConstructor(new Type[0]);
                Assert.IsNotNull(defCtor);

                var ex = (Exception) defCtor.Invoke(new object[0]);
                Assert.AreEqual(string.Format("Exception of type '{0}' was thrown.", type.FullName), ex.Message);

                // Message ctor.
                var msgCtor = type.GetConstructor(new[] {typeof(string)});
                Assert.IsNotNull(msgCtor);

                ex = (Exception) msgCtor.Invoke(new object[] {"myMessage"});
                Assert.AreEqual("myMessage", ex.Message);

                // Serialization.
                var stream = new MemoryStream();
                var formatter = new BinaryFormatter();

                formatter.Serialize(stream, ex);
                stream.Seek(0, SeekOrigin.Begin);

                ex = (Exception) formatter.Deserialize(stream);
                Assert.AreEqual("myMessage", ex.Message);

                // Message+cause ctor.
                var msgCauseCtor = type.GetConstructor(new[] { typeof(string), typeof(Exception) });
                Assert.IsNotNull(msgCauseCtor);

                ex = (Exception) msgCauseCtor.Invoke(new object[] {"myMessage", new Exception("innerEx")});
                Assert.AreEqual("myMessage", ex.Message);
                Assert.IsNotNull(ex.InnerException);
                Assert.AreEqual("innerEx", ex.InnerException.Message);
            }
        }

        /// <summary>
        /// Tests CachePartialUpdateException serialization.
        /// </summary>
        private static void TestPartialUpdateExceptionSerialization(Exception ex)
        {
            var formatter = new BinaryFormatter();

            var stream = new MemoryStream();

            formatter.Serialize(stream, ex);

            stream.Seek(0, SeekOrigin.Begin);

            var ex0 = (Exception) formatter.Deserialize(stream);

            var updateEx = ((CachePartialUpdateException) ex);

            try
            {
                Assert.AreEqual(updateEx.GetFailedKeys<object>(),
                    ((CachePartialUpdateException) ex0).GetFailedKeys<object>());
            }
            catch (Exception e)
            {
                if (typeof(IgniteException) != e.GetType())
                    throw;
            }

            while (ex != null && ex0 != null)
            {
                Assert.AreEqual(ex0.GetType(), ex.GetType());
                Assert.AreEqual(ex.Message, ex0.Message);

                ex = ex.InnerException;
                ex0 = ex0.InnerException;
            }

            Assert.AreEqual(ex, ex0);
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPartialUpdateExceptionAsync()
        {
            // Primitive type
            TestPartialUpdateException(true, (x, g) => x);

            // User type
            TestPartialUpdateException(true, (x, g) => new BinarizableEntry(x));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation in binary mode.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestPartialUpdateExceptionAsyncBinarizable()
        {
            TestPartialUpdateException(true, (x, g) => g.GetBinary().ToBinary<IBinaryObject>(new BinarizableEntry(x)));
        }

        /// <summary>
        /// Tests that invalid spring URL results in a meaningful exception.
        /// </summary>
        [Test]
        public void TestInvalidSpringUrl()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = "z:\\invalid.xml"
            };

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.IsTrue(ex.ToString().Contains("Spring XML configuration path is invalid: z:\\invalid.xml"));
        }

        /// <summary>
        /// Tests that invalid configuration parameter results in a meaningful exception.
        /// </summary>
        [Test]
        public void TestInvalidConfig()
        {
            var disco = TestUtils.GetStaticDiscovery();
            disco.SocketTimeout = TimeSpan.FromSeconds(-1);  // set invalid timeout

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DiscoverySpi = disco
            };

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.IsTrue(ex.ToString().Contains("SPI parameter failed condition check: sockTimeout > 0"));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation.
        /// </summary>
        private static void TestPartialUpdateException<TK>(bool async, Func<int, IIgnite, TK> keyFunc)
        {
            using (var grid = StartGrid())
            {
                var cache = grid.GetCache<TK, int>("partitioned_atomic").WithNoRetries();

                if (typeof (TK) == typeof (IBinaryObject))
                    cache = cache.WithKeepBinary<TK, int>();

                // Do cache puts in parallel
                var putTask = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        // Do a lot of puts so that one fails during Ignite stop
                        for (var i = 0; i < 1000000; i++)
                        {
                            // ReSharper disable once AccessToDisposedClosure
                            // ReSharper disable once AccessToModifiedClosure
                            var dict = Enumerable.Range(1, 100).ToDictionary(k => keyFunc(k, grid), k => i);

                            if (async)
                                cache.PutAllAsync(dict).Wait();
                            else
                                cache.PutAll(dict);
                        }
                    }
                    catch (AggregateException ex)
                    {
                        CheckPartialUpdateException<TK>((CachePartialUpdateException) ex.InnerException);

                        return;
                    }
                    catch (CachePartialUpdateException ex)
                    {
                        CheckPartialUpdateException<TK>(ex);

                        return;
                    }

                    Assert.Fail("CachePartialUpdateException has not been thrown.");
                });

                while (true)
                {
                    Thread.Sleep(1000);

                    Ignition.Stop("grid_2", true);
                    StartGrid("grid_2");

                    Thread.Sleep(1000);

                    if (putTask.Exception != null)
                        throw putTask.Exception;

                    if (putTask.IsCompleted)
                        return;
                }
            }
        }

        /// <summary>
        /// Checks the partial update exception.
        /// </summary>
        private static void CheckPartialUpdateException<TK>(CachePartialUpdateException ex)
        {
            var failedKeys = ex.GetFailedKeys<TK>();

            Assert.IsTrue(failedKeys.Any());

            var failedKeysObj = ex.GetFailedKeys<object>();

            Assert.IsTrue(failedKeysObj.Any());
        }

        /// <summary>
        /// Starts the grid.
        /// </summary>
        private static IIgnite StartGrid(string gridName = null)
        {
            return Ignition.Start(new IgniteConfiguration
            {
                SpringConfigUrl = "config\\native-client-test-cache.xml",
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath(),
                GridName = gridName,
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof (BinarizableEntry))
                    }
                }
            });
        }

        /// <summary>
        /// Binarizable entry.
        /// </summary>
        private class BinarizableEntry
        {
            /** Value. */
            private readonly int _val;

            /** <inheritDot /> */
            public override int GetHashCode()
            {
                return _val;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="val">Value.</param>
            public BinarizableEntry(int val)
            {
                _val = val;
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                return obj is BinarizableEntry && ((BinarizableEntry)obj)._val == _val;
            }
        }

        /// <summary>
        /// Serializable entry.
        /// </summary>
        [Serializable]
        private class SerializableEntry
        {
            /** Value. */
            private readonly int _val;

            /** <inheritDot /> */
            public override int GetHashCode()
            {
                return _val;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="val">Value.</param>
            public SerializableEntry(int val)
            {
                _val = val;
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                return obj is SerializableEntry && ((SerializableEntry)obj)._val == _val;
            }
        }
    }
}
