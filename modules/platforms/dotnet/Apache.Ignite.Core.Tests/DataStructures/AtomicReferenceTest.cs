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

namespace Apache.Ignite.Core.Tests.DataStructures
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Atomic reference test.
    /// </summary>
    public class AtomicReferenceTest : SpringTestBase
    {
        /** */
        private const string AtomicRefName = "testAtomicRef";

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicReferenceTest"/> class.
        /// </summary>
        public AtomicReferenceTest() : base("Config\\Compute\\compute-grid1.xml")
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void TestSetUp()
        {
            base.TestSetUp();

            // Close test atomic if there is any
            Grid.GetAtomicReference(AtomicRefName, 0, true).Close();
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetConfiguration(string springConfigUrl)
        {
            var cfg = base.GetConfiguration(springConfigUrl);

            cfg.BinaryConfiguration = new BinaryConfiguration(typeof(BinaryObj));

            return cfg;
        }

        /// <summary>
        /// Tests lifecycle of the AtomicReference.
        /// </summary>
        [Test]
        public void TestCreateClose()
        {
            Assert.IsNull(Grid.GetAtomicReference(AtomicRefName, 10, false));

            // Nonexistent atomic returns null
            Assert.IsNull(Grid.GetAtomicReference(AtomicRefName, 10, false));

            // Create new
            var al = Grid.GetAtomicReference(AtomicRefName, 10, true);
            Assert.AreEqual(AtomicRefName, al.Name);
            Assert.AreEqual(10, al.Read());
            Assert.AreEqual(false, al.IsClosed);

            // Get existing with create flag
            var al2 = Grid.GetAtomicReference(AtomicRefName, 5, true);
            Assert.AreEqual(AtomicRefName, al2.Name);
            Assert.AreEqual(10, al2.Read());
            Assert.AreEqual(false, al2.IsClosed);

            // Get existing without create flag
            var al3 = Grid.GetAtomicReference(AtomicRefName, 5, false);
            Assert.AreEqual(AtomicRefName, al3.Name);
            Assert.AreEqual(10, al3.Read());
            Assert.AreEqual(false, al3.IsClosed);

            al.Close();

            Assert.AreEqual(true, al.IsClosed);
            Assert.AreEqual(true, al2.IsClosed);
            Assert.AreEqual(true, al3.IsClosed);

            Assert.IsNull(Grid.GetAtomicReference(AtomicRefName, 10, false));
        }

        /// <summary>
        /// Tests modification methods.
        /// </summary>
        [Test]
        public void TestModify()
        {
            var atomics = Enumerable.Range(1, 10)
                .Select(x => Grid.GetAtomicReference(AtomicRefName, 5, true)).ToList();

            atomics.ForEach(x => Assert.AreEqual(5, x.Read()));

            atomics[0].Write(15);
            atomics.ForEach(x => Assert.AreEqual(15, x.Read()));

            Assert.AreEqual(15, atomics[0].CompareExchange(42, 15));
            atomics.ForEach(x => Assert.AreEqual(42, x.Read()));
        }

        /// <summary>
        /// Tests primitives in the atomic.
        /// </summary>
        [Test]
        public void TestPrimitives()
        {
            TestOperations(1, 2);
            TestOperations("1", "2");
            TestOperations(Guid.NewGuid(), Guid.NewGuid());
        }

        /// <summary>
        /// Tests DateTime in the atomic.
        /// </summary>
        [Test]
        public void TestDateTime()
        {
            TestOperations(DateTime.Now, DateTime.Now.AddDays(-1));
        }

        /// <summary>
        /// Tests serializable objects in the atomic.
        /// </summary>
        [Test]
        public void TestSerializable()
        {
            TestOperations(new SerializableObj {Foo = 16}, new SerializableObj {Foo = -5});
        }

        /// <summary>
        /// Tests binarizable objects in the atomic.
        /// </summary>
        [Test]
        public void TestBinarizable()
        {
            TestOperations(new BinaryObj {Foo = 16}, new BinaryObj {Foo = -5});
        }

        /// <summary>
        /// Tests operations on specific object.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="x">The x.</param>
        /// <param name="y">The y.</param>
        private void TestOperations<T>(T x, T y)
        {
            Grid.GetAtomicReference(AtomicRefName, 0, true).Close();

            var atomic = Grid.GetAtomicReference(AtomicRefName, x, true);

            Assert.AreEqual(x, atomic.Read());

            atomic.Write(y);
            Assert.AreEqual(y, atomic.Read());

            var old = atomic.CompareExchange(x, y);
            Assert.AreEqual(y, old);
            Assert.AreEqual(x, atomic.Read());

            old = atomic.CompareExchange(x, y);
            Assert.AreEqual(x, old);
            Assert.AreEqual(x, atomic.Read());

            // Check nulls
            var nul = default(T);

            old = atomic.CompareExchange(nul, x);
            Assert.AreEqual(x, old);
            Assert.AreEqual(nul, atomic.Read());

            old = atomic.CompareExchange(y, nul);
            Assert.AreEqual(nul, old);
            Assert.AreEqual(y, atomic.Read());
        }

        /// <summary>
        /// Serializable.
        /// </summary>
        [Serializable]
        private class SerializableObj
        {
            /** */
            public int Foo { get; set; }

            /// <summary>
            /// Determines whether the specified object is equal to the current object.
            /// </summary>
            private bool Equals(SerializableObj other)
            {
                return Foo == other.Foo;
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((SerializableObj) obj);
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                // ReSharper disable once NonReadonlyMemberInGetHashCode
                return Foo;
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return base.ToString() + "[" + Foo + "]";
            }
        }

        /// <summary>
        /// Binary.
        /// </summary>
        private sealed class BinaryObj : SerializableObj
        {
            // No-op.
        }
    }
}