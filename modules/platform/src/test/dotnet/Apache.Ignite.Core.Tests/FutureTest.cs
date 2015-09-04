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
    using System.Collections.Generic;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// Future tests.
    /// </summary>
    public class FutureTest
    {
        /** */
        private ICache<object, object> _cache;

        /** */
        private ICompute _compute;

        /// <summary>
        /// Test fixture set-up routine.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            TestUtils.KillProcesses();

            var grid = Ignition.Start(new IgniteConfiguration
            {
                SpringConfigUrl = "config\\compute\\compute-standalone.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                PortableConfiguration = new PortableConfiguration
                {
                    TypeConfigurations =
                        new List<PortableTypeConfiguration> { new PortableTypeConfiguration(typeof(Portable)) }
                }
            });

            _cache = grid.Cache<object, object>(null).WithAsync();

            _compute = grid.Compute().WithAsync();
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            TestUtils.KillProcesses();
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
            _compute.Broadcast(new SleepAction());

            var fut = _compute.GetFuture<object>();

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
            _cache.Put(1, 1);

            _cache.GetFuture().ToTask().Wait();

            _cache.Get(1);

            var task1 = _cache.GetFuture<int>().ToTask();

            Assert.AreEqual(1, task1.Result);

            Assert.IsTrue(task1.IsCompleted);

            _compute.Broadcast(new SleepAction());

            var task2 = _compute.GetFuture().ToTask();

            Assert.IsFalse(task2.IsCompleted);

            Assert.IsFalse(task2.Wait(100));

            task2.Wait();

            Assert.IsTrue(task2.IsCompleted);

            Assert.AreEqual(null, task2.Result);
        }

        [Test]
        public void TestGetWithTimeout()
        {
            _compute.Broadcast(new SleepAction());

            var fut = _compute.GetFuture();

            Assert.Throws<TimeoutException>(() => fut.Get(TimeSpan.FromMilliseconds(100)));

            fut.Get(TimeSpan.FromSeconds(1));

            Assert.IsTrue(fut.IsDone);
        }

        [Test]
        public void TestToAsyncResult()
        {
            _compute.Broadcast(new SleepAction());

            IFuture fut = _compute.GetFuture();

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

            TestType(new Portable { A = 10, B = "foo" });
        }

        /// <summary>
        /// Tests future type.
        /// </summary>
        private void TestType<T>(T value)
        {
            var key = typeof(T).Name;

            _cache.Put(key, value);

            _cache.GetFuture().Get();

            _cache.Get(key);

            Assert.AreEqual(value, _cache.GetFuture<T>().Get());
        }

        /// <summary>
        /// Portable test class.
        /// </summary>
        private class Portable : IPortableMarshalAware
        {
            public int A;
            public string B;

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteInt("a", A);
                writer.RawWriter().WriteString(B);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                A = reader.ReadInt("a");
                B = reader.RawReader().ReadString();
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

                return A == other.A && string.Equals(B, other.B);
            }

            /** <inheritDoc /> */
            public override int GetHashCode()
            {
                unchecked
                {
                    // ReSharper disable NonReadonlyMemberInGetHashCode
                    return (A * 397) ^ (B != null ? B.GetHashCode() : 0);
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