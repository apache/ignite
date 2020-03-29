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
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Atomic long test.
    /// </summary>
    public class AtomicLongTest : SpringTestBase
    {
        /** */
        private const string AtomicLongName = "testAtomicLong";

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicLongTest"/> class.
        /// </summary>
        public AtomicLongTest() : base("Config\\Compute\\compute-grid1.xml")
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void TestSetUp()
        {
            base.TestSetUp();

            // Close test atomic if there is any
            Grid.GetAtomicLong(AtomicLongName, 0, true).Close();
        }

        /// <summary>
        /// Tests lifecycle of the AtomicLong.
        /// </summary>
        [Test]
        public void TestCreateClose()
        {
            // Nonexistent long returns null
            Assert.IsNull(Grid.GetAtomicLong(AtomicLongName, 10, false));

            // Create new
            var al = Grid.GetAtomicLong(AtomicLongName, 10, true);
            Assert.AreEqual(AtomicLongName, al.Name);
            Assert.AreEqual(10, al.Read());
            Assert.AreEqual(false, al.IsClosed());

            // Get existing with create flag
            var al2 = Grid.GetAtomicLong(AtomicLongName, 5, true);
            Assert.AreEqual(AtomicLongName, al2.Name);
            Assert.AreEqual(10, al2.Read());
            Assert.AreEqual(false, al2.IsClosed());

            // Get existing without create flag
            var al3 = Grid.GetAtomicLong(AtomicLongName, 5, false);
            Assert.AreEqual(AtomicLongName, al3.Name);
            Assert.AreEqual(10, al3.Read());
            Assert.AreEqual(false, al3.IsClosed());

            al.Close();

            Assert.AreEqual(true, al.IsClosed());
            Assert.AreEqual(true, al2.IsClosed());
            Assert.AreEqual(true, al3.IsClosed());

            Assert.IsNull(Grid.GetAtomicLong(AtomicLongName, 10, false));
        }

        /// <summary>
        /// Tests modification methods.
        /// </summary>
        [Test]
        public void TestModify()
        {
            var atomics = Enumerable.Range(1, 10)
                .Select(x => Grid.GetAtomicLong(AtomicLongName, 5, true)).ToList();

            atomics.ForEach(x => Assert.AreEqual(5, x.Read()));

            Assert.AreEqual(10, atomics[0].Add(5));
            atomics.ForEach(x => Assert.AreEqual(10, x.Read()));

            Assert.AreEqual(10, atomics[0].CompareExchange(33, 10));  // successful exchange
            atomics.ForEach(x => Assert.AreEqual(33, x.Read()));

            Assert.AreEqual(33, atomics[0].CompareExchange(44, 10));  // failed exchange
            atomics.ForEach(x => Assert.AreEqual(33, x.Read()));

            Assert.AreEqual(33, atomics[0].Exchange(42));
            atomics.ForEach(x => Assert.AreEqual(42, x.Read()));

            Assert.AreEqual(41, atomics[0].Decrement());
            atomics.ForEach(x => Assert.AreEqual(41, x.Read()));
            
            Assert.AreEqual(42, atomics[0].Increment());
            atomics.ForEach(x => Assert.AreEqual(42, x.Read()));
        }

        /// <summary>
        /// Tests multithreaded scenario.
        /// </summary>
        [Test]
        [Category(TestUtils.CategoryIntensive)]
        public void TestMultithreaded()
        {
            const int atomicCnt = 10;
            const int threadCnt = 5;
            const int iterations = 3000;

            // 10 atomics with same name
            var atomics = Enumerable.Range(1, atomicCnt)
                .Select(x => Grid.GetAtomicLong(AtomicLongName, 0, true)).ToList();

            // 5 threads increment 30000 times
            TestUtils.RunMultiThreaded(() =>
            {
                for (var i = 0; i < iterations; i++)
                    atomics.ForEach(x => x.Increment());
            }, threadCnt);

            atomics.ForEach(x => Assert.AreEqual(atomicCnt*threadCnt*iterations, x.Read()));
        }
    }
}
