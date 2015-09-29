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
    public class AtomicLongTest : IgniteTestBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicLongTest"/> class.
        /// </summary>
        public AtomicLongTest() : base("config\\compute\\compute-grid1.xml")
        {
            // No-op.
        }

        /// <summary>
        /// Tests lifecycle of the AtomicLong.
        /// </summary>
        [Test]
        public void TestCreateClose()
        {
            var al = Grid.GetAtomicLong("test", 10, true);

            Assert.AreEqual("test", al.Name);
            Assert.AreEqual(10, al.Read());
            Assert.AreEqual(false, al.IsClosed());

            var al2 = Grid.GetAtomicLong("test", 5, true);
            Assert.AreEqual("test", al2.Name);
            Assert.AreEqual(10, al2.Read());
            Assert.AreEqual(false, al2.IsClosed());

            al.Close();

            Assert.AreEqual(true, al.IsClosed());
            Assert.AreEqual(true, al2.IsClosed());
        }

        /// <summary>
        /// Tests multithreaded scenario.
        /// </summary>
        [Test]
        public void TestMultithreaded()
        {
            // TODO: Profile.

            const int atomicCnt = 10;
            const int threadCnt = 5;
            const int iterations = 3000;

            // 10 atomics with same name
            var atomics = Enumerable.Range(1, atomicCnt).Select(x => Grid.GetAtomicLong("test", 0, true)).ToList();

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
