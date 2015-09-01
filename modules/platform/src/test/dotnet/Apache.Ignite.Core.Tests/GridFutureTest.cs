/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using GridGain.Cache;
    using GridGain.Common;
    using GridGain.Compute;
    using GridGain.Portable;

    using NUnit.Framework;

    public class GridFutureTest
    {
        /** */
        private ICache<object, object> cache;

        /** */
        private ICompute compute;

        /// <summary>
        /// Test fixture set-up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            GridTestUtils.KillProcesses();

            var grid = GridFactory.Start(new GridConfiguration
            {
                SpringConfigUrl = "config\\compute\\compute-standalone.xml",
                JvmClasspath = GridTestUtils.CreateTestClasspath(),
                JvmOptions = GridTestUtils.TestJavaOptions(),
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations =
                        new List<PortableTypeConfiguration> { new PortableTypeConfiguration(typeof(Portable)) }
                }
            });

            cache = grid.Cache<object, object>(null).WithAsync();

            compute = grid.Compute().WithAsync();
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            GridTestUtils.KillProcesses();
        }

        [Test]
        public void TestListen()
        {
            // Listen(Action callback)
            TestListen((fut, act) => fut.Listen(act));

            // Listen(Action<IFuture> callback)
            TestListen((fut, act) => ((IFuture)fut).Listen(f =>
            {
                Assert.AreEqual(f, fut);
                act();
            }));

            // Listen(Action<IFuture<T>> callback)
            TestListen((fut, act) => fut.Listen(f =>
            {
                Assert.AreEqual(f, fut);
                act();
            }));
        }

        private void TestListen(Action<IFuture<object>, Action> listenAction)
        {
            compute.Broadcast(new SleepAction());

            var fut = compute.GetFuture<object>();

            var listenCount = 0;

            // Multiple subscribers before completion
            for (var i = 0; i < 10; i++)
                listenAction(fut, () => Interlocked.Increment(ref listenCount));

            Assert.IsFalse(fut.IsDone);

            Assert.IsNull(fut.Get());

            Thread.Sleep(100);  // wait for future completion thread

            Assert.AreEqual(10, listenCount);

            // Multiple subscribers after completion
            for (var i = 0; i < 10; i++)
                listenAction(fut, () => Interlocked.Decrement(ref listenCount));

            Assert.AreEqual(0, listenCount);
        }

        [Test]
        public void TestToTask()
        {
            cache.Put(1, 1);

            cache.GetFuture().ToTask().Wait();

            cache.Get(1);

            var task1 = cache.GetFuture<int>().ToTask();

            Assert.AreEqual(1, task1.Result);

            Assert.IsTrue(task1.IsCompleted);

            compute.Broadcast(new SleepAction());

            var task2 = compute.GetFuture().ToTask();

            Assert.IsFalse(task2.IsCompleted);

            Assert.IsFalse(task2.Wait(100));

            task2.Wait();

            Assert.IsTrue(task2.IsCompleted);

            Assert.AreEqual(null, task2.Result);
        }

        [Test]
        public void TestGetWithTimeout()
        {
            compute.Broadcast(new SleepAction());

            var fut = compute.GetFuture();

            Assert.Throws<TimeoutException>(() => fut.Get(TimeSpan.FromMilliseconds(100)));

            fut.Get(TimeSpan.FromSeconds(1));

            Assert.IsTrue(fut.IsDone);
        }

        [Test]
        public void TestToAsyncResult()
        {
            compute.Broadcast(new SleepAction());

            IFuture fut = compute.GetFuture();

            var asyncRes = fut.ToAsyncResult();

            Assert.IsFalse(asyncRes.IsCompleted);

            Assert.IsTrue(asyncRes.AsyncWaitHandle.WaitOne(1000));

            Assert.IsTrue(asyncRes.IsCompleted);
        }

        [Test]
        public void TestFutureTypes()
        {
            TestType(false);
            TestType((byte)11);
            TestType('x'); // char
            TestType(2.7d); // double
            TestType(3.14f); // float
            TestType(16); // int
            TestType(17L); // long
            TestType((short)18);

            TestType(18m); // decimal

            TestType(new Portable { a = 10, b = "foo" });
        }

        /// <summary>
        /// Tests future type.
        /// </summary>
        private void TestType<T>(T value)
        {
            var key = typeof(T).Name;

            cache.Put(key, value);

            cache.GetFuture().Get();

            cache.Get(key);

            Assert.AreEqual(value, cache.GetFuture<T>().Get());
        }

        /// <summary>
        /// Portable test class.
        /// </summary>
        private class Portable : IPortableMarshalAware
        {
            public int a;
            public string b;

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteInt("a", a);
                writer.RawWriter().WriteString(b);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                a = reader.ReadInt("a");
                b = reader.RawReader().ReadString();
            }

            /** <inheritDoc /> */
            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;

                if (ReferenceEquals(this, obj))
                    return true;

                if (obj.GetType() != GetType())
                    return false;

                var other = (Portable)obj;

                return a == other.a && string.Equals(b, other.b);
            }

            /** <inheritDoc /> */
            public override int GetHashCode()
            {
                unchecked
                {
                    // ReSharper disable NonReadonlyMemberInGetHashCode
                    return (a * 397) ^ (b != null ? b.GetHashCode() : 0);
                    // ReSharper restore NonReadonlyMemberInGetHashCode
                }
            }
        }

        /// <summary>
        /// Compute action with a delay to ensure lengthy future execution.
        /// </summary>
        [Serializable]
        private class SleepAction : IComputeAction
        {
            public void Invoke()
            {
                Thread.Sleep(500);
            }
        }
    }
}