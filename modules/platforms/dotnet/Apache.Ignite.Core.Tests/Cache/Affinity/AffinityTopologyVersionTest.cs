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

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using Apache.Ignite.Core.Cache.Affinity;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="AffinityTopologyVersion"/>
    /// </summary>
    public class AffinityTopologyVersionTest
    {
        /// <summary>
        /// Tests the equality.
        /// </summary>
        [Test]
        public void TestEquality()
        {
            // Default.
            var ver = new AffinityTopologyVersion();

            Assert.AreEqual(0, ver.Version);
            Assert.AreEqual(0, ver.MinorVersion);
            Assert.AreEqual(0, ver.GetHashCode());
            Assert.AreEqual(new AffinityTopologyVersion(), ver);

            // Custom.
            var ver1 = new AffinityTopologyVersion(long.MaxValue, int.MaxValue);
            var ver2 = new AffinityTopologyVersion(long.MaxValue, int.MaxValue);

            Assert.AreEqual(ver1, ver2);
            Assert.IsTrue(ver1 == ver2);
            Assert.IsFalse(ver1 != ver2);

            Assert.AreNotEqual(ver, ver1);
            Assert.IsTrue(ver != ver1);
            Assert.IsFalse(ver == ver1);

            // ToString.
            Assert.AreEqual("AffinityTopologyVersion [Version=1, MinorVersion=2]", 
                new AffinityTopologyVersion(1, 2).ToString());
        }
    }
}
