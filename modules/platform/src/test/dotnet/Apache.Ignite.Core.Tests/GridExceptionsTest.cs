/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Tests 
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Tests grid exceptions propagation.
    /// </summary>
    public class IgniteExceptionsTest
    {
        /// <summary>
        /// Before test.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            GridTestUtils.KillProcesses();
        }
        
        /// <summary>
        /// After test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            GridFactory.StopAll(true);
        }

        /// <summary>
        /// Tests exceptions.
        /// </summary>
        [Test]
        public void TestExceptions()
        {
            var grid = StartGrid();

            try
            {
                grid.Cache<object, object>("invalidCacheName");

                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsTrue(e is ArgumentException);
            }

            try
            {
                grid.Cluster.ForRemotes().Metrics();

                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsTrue(e is ClusterGroupEmptyException);
            }

            grid.Dispose();

            try
            {
                grid.Cache<object, object>("cache1");

                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.IsTrue(e is InvalidOperationException);
            }
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation.
        /// </summary>
        [Test]
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestPartialUpdateException()
        {
            // Primitive type
            TestPartialUpdateException(false, (x, g) => x);

            // User type
            TestPartialUpdateException(false, (x, g) => new PortableEntry(x));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation in portable mode.
        /// </summary>
        [Test]
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestPartialUpdateExceptionPortable()
        {
            // User type
            TestPartialUpdateException(false, (x, g) => g.Portables().ToPortable<IPortableObject>(new PortableEntry(x)));
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
                    ((CachePartialUpdateException)ex0).GetFailedKeys<object>());
            }
            catch (Exception e)
            {
                if (typeof (IgniteException) != e.GetType())
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
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestPartialUpdateExceptionAsync()
        {
            // Primitive type
            TestPartialUpdateException(true, (x, g) => x);

            // User type
            TestPartialUpdateException(true, (x, g) => new PortableEntry(x));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation in portable mode.
        /// </summary>
        [Test]
        [Category(GridTestUtils.CATEGORY_INTENSIVE)]
        public void TestPartialUpdateExceptionAsyncPortable()
        {
            TestPartialUpdateException(true, (x, g) => g.Portables().ToPortable<IPortableObject>(new PortableEntry(x)));
        }

        /// <summary>
        /// Tests CachePartialUpdateException keys propagation.
        /// </summary>
        private static void TestPartialUpdateException<K>(bool async, Func<int, IIgnite, K> keyFunc)
        {
            using (var grid = StartGrid())
            {
                var cache = grid.Cache<K, int>("partitioned_atomic").WithNoRetries();

                if (async)
                    cache = cache.WithAsync();

                if (typeof (K) == typeof (IPortableObject))
                    cache = cache.WithKeepPortable<K, int>();

                // Do cache puts in parallel
                var putTask = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        // Do a lot of puts so that one fails during grid stop
                        for (var i = 0; i < 1000000; i++)
                        {
                            cache.PutAll(Enumerable.Range(1, 100).ToDictionary(k => keyFunc(k, grid), k => i));

                            if (async)
                                cache.GetFuture().Get();
                        }
                    }
                    catch (CachePartialUpdateException ex)
                    {
                        var failedKeys = ex.GetFailedKeys<K>();

                        Assert.IsTrue(failedKeys.Any());

                        var failedKeysObj = ex.GetFailedKeys<object>();

                        Assert.IsTrue(failedKeysObj.Any());

                        return;
                    }

                    Assert.Fail("CachePartialUpdateException has not been thrown.");
                });

                while (true)
                {
                    GridFactory.Stop("grid_2", true);
                    StartGrid("grid_2");

                    if (putTask.Exception != null)
                        throw putTask.Exception;

                    if (putTask.IsCompleted)
                        return;
                }
            }
        }

        /// <summary>
        /// Starts the grid.
        /// </summary>
        private static IIgnite StartGrid(string gridName = null)
        {
            return GridFactory.Start(new GridConfigurationEx
            {
                SpringConfigUrl = "config\\native-client-test-cache.xml",
                JvmOptions = GridTestUtils.TestJavaOptions(),
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                GridName = gridName,
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new PortableTypeConfiguration(typeof (PortableEntry))
                    }
                }
            });
        }

        /// <summary>
        /// Portable entry.
        /// </summary>
        private class PortableEntry
        {
            /** Value. */
            private readonly int val;

            /** <inheritDot /> */
            public override int GetHashCode()
            {
                return val;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="val">Value.</param>
            public PortableEntry(int val)
            {
                this.val = val;
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                return obj is PortableEntry && ((PortableEntry)obj).val == val;
            }
        }

        /// <summary>
        /// Portable entry.
        /// </summary>
        [Serializable]
        private class SerializableEntry
        {
            /** Value. */
            private readonly int val;

            /** <inheritDot /> */
            public override int GetHashCode()
            {
                return val;
            }

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="val">Value.</param>
            public SerializableEntry(int val)
            {
                this.val = val;
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                return obj is SerializableEntry && ((SerializableEntry)obj).val == val;
            }
        }
    }
}
