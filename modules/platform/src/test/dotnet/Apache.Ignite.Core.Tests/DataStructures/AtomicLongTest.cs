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
    using Apache.Ignite.Core.DataStructures;
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
        public void TestCreate()
        {
            IAtomicLong al;

            using (al = Grid1.GetAtomicLong("test", 10, true))
            {
                Assert.AreEqual("test", al.Name);
                Assert.AreEqual(10, al.Read());
                Assert.AreEqual(false, al.IsRemoved());

                using (var al2 = Grid1.GetAtomicLong("test", 0, false))
                {
                    Assert.AreEqual("test", al2.Name);
                    Assert.AreEqual(10, al2.Read());
                    Assert.AreEqual(false, al2.IsRemoved());
                }

                // TODO: This is wrong. IDisposable does not apply here.
                Assert.AreEqual(true, al.IsRemoved());
            }

            Assert.Throws<ObjectDisposedException>(() => al.IsRemoved());
        }
    }
}
