/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.EntityFramework.Tests
{
    using System;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="DbCachingPolicy"/>.
    /// </summary>
    public class DbCachingPolicyTest
    {
        /// <summary>
        /// Tests the default implementation.
        /// </summary>
        [Test]
        public void TestDefaultImpl()
        {
            var plc = new DbCachingPolicy();

            Assert.IsTrue(plc.CanBeCached(null));
            Assert.IsTrue(plc.CanBeCached(null, 0));
            Assert.AreEqual(TimeSpan.MaxValue, plc.GetExpirationTimeout(null));
            Assert.AreEqual(DbCachingMode.ReadWrite, plc.GetCachingMode(null));
        }
    }
}
