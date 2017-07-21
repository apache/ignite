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
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Compute;
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
            var grid = Ignition.Start(TestUtils.GetTestConfiguration());

            _cache = grid.CreateCache<object, object>("cache");

            _compute = grid.GetCompute();
        }

        /// <summary>
        /// Test fixture tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);
        }


        [Test]
        public void TestToTask()
        {
            _cache.PutAsync(1, 1).Wait();

            var task1 = _cache.GetAsync(1);

            Assert.AreEqual(1, task1.Result);

            Assert.IsTrue(task1.IsCompleted);

            var task2 = _compute.BroadcastAsync(new SleepAction());

            Assert.IsFalse(task2.IsCompleted);

            Assert.IsFalse(task2.Wait(100));

            task2.Wait();

            Assert.IsTrue(task2.IsCompleted);
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

            TestType(new Binarizable { A = 10, B = "foo" });
        }

        /// <summary>
        /// Tests future type.
        /// </summary>
        private void TestType<T>(T value)
        {
            var key = typeof(T).Name;

            _cache.PutAsync(key, value).Wait();

            Assert.AreEqual(value, _cache.GetAsync(key).Result);
        }

        /// <summary>
        /// Binary test class.
        /// </summary>
        private class Binarizable : IBinarizable
        {
            public int A;
            public string B;

            /** <inheritDoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("a", A);
                writer.GetRawWriter().WriteString(B);
            }

            /** <inheritDoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                A = reader.ReadInt("a");
                B = reader.GetRawReader().ReadString();
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

                var other = (Binarizable)obj;

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