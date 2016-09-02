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

namespace Apache.Ignite.Core.Tests.AspNet
{
    using System;
    using Apache.Ignite.Core.Impl.AspNet;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="SessionStateData"/>.
    /// </summary>
    public class BinarizableSessionStateStoreDataTest
    {
        /// <summary>
        /// Tests the empty data.
        /// </summary>
        [Test]
        public void TestEmpty()
        {
            var data = new SessionStateData();

            Assert.AreEqual(0, data.LockId);
            Assert.AreEqual(0, data.Items.Count);
            Assert.AreEqual(0, data.Timeout);
            Assert.IsNull(data.LockNodeId);
            Assert.IsNull(data.LockTime);
            Assert.IsNull(data.StaticObjects);
        }

        /// <summary>
        /// Tests the serialization.
        /// </summary>
        [Test]
        public void TestSerialization()
        {
            var data = new SessionStateData
            {
                Timeout = 97,
                LockId = 11,
                LockNodeId = Guid.NewGuid(),
                LockTime = DateTime.UtcNow.AddHours(-1),
                StaticObjects = new byte[] {1, 2, 3}
            };

            data.Items["key1"] = 1;
            data.Items["key2"] = 2;

            var data0 = TestUtils.SerializeDeserialize(data);

            Assert.AreEqual(data.Timeout, data0.Timeout);
            Assert.AreEqual(data.LockId, data0.LockId);
            Assert.AreEqual(data.LockNodeId, data0.LockNodeId);
            Assert.AreEqual(data.LockTime, data0.LockTime);
            Assert.AreEqual(data.StaticObjects, data0.StaticObjects);
            Assert.AreEqual(data.Items.GetKeys(), data0.Items.GetKeys());
        }
    }
}
