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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientFeatures"/> class.
    /// </summary>
    public class ClientFeaturesTest
    {
        /// <summary>
        /// Tests that <see cref="ClientFeatures.GetMinVersion"/> returns a version
        /// for every valid <see cref="ClientOp"/>.
        /// </summary>
        [Test]
        public void TestGetMinVersionReturnsValueForEveryValidOp()
        {
            foreach (ClientOp clientOp in Enum.GetValues(typeof(ClientOp)))
            {
                var minVersion = ClientFeatures.GetMinVersion(clientOp);
                
                Assert.IsTrue(minVersion >= ClientSocket.Ver100);
                Assert.IsTrue(minVersion <= ClientSocket.CurrentProtocolVersion);
            }
        }

        /// <summary>
        /// Tests that <see cref="ClientFeatures.GetMinVersion"/> returns a specific version for known new features.
        /// </summary>
        [Test]
        public void TestGetMinVersionReturnsSpecificVersionForNewFeatures()
        {
            Assert.AreEqual(ClientSocket.Ver140, ClientFeatures.GetMinVersion(ClientOp.CachePartitions));

            Assert.AreEqual(ClientSocket.Ver150, ClientFeatures.GetMinVersion(ClientOp.ClusterIsActive));
            Assert.AreEqual(ClientSocket.Ver150, ClientFeatures.GetMinVersion(ClientOp.ClusterChangeState));
            Assert.AreEqual(ClientSocket.Ver150, ClientFeatures.GetMinVersion(ClientOp.ClusterChangeWalState));
            Assert.AreEqual(ClientSocket.Ver150, ClientFeatures.GetMinVersion(ClientOp.ClusterGetWalState));
        }

        /// <summary>
        /// Tests <see cref="ClientFeatures.AllFeatures"/>.
        /// </summary>
        [Test]
        public void TestAllFeatures()
        {
            var expected = Enum.GetValues(typeof(ClientBitmaskFeature))
                .Cast<int>()
                .Aggregate(0, (a, b) => a | (1 << b));
            
            Assert.AreEqual(expected, ClientFeatures.AllFeatures.Single());
        }

        /// <summary>
        /// Tests <see cref="ClientFeatures.HasFeature"/>.
        /// </summary>
        [Test]
        public void TestHasFeature()
        {
            var features = ClientFeatures.CurrentFeatures;
            
            Assert.IsTrue(features.HasFeature(ClientBitmaskFeature.ClusterGroupGetNodesEndpoints));
            Assert.IsFalse(features.HasFeature((ClientBitmaskFeature) (-1)));
            Assert.IsFalse(features.HasFeature((ClientBitmaskFeature) 12345));
        }
    }
}