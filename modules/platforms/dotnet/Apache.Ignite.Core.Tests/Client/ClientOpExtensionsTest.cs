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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClientOpExtensions"/> class.
    /// </summary>
    public class ClientOpExtensionsTest
    {
        /// <summary>
        /// Tests that <see cref="ClientOpExtensions.GetMinVersion"/> returns a version
        /// for every valid <see cref="ClientOp"/>.
        /// </summary>
        [Test]
        public void TestGetMinVersionReturnsValueForEveryValidOp()
        {
            foreach (ClientOp clientOp in Enum.GetValues(typeof(ClientOp)))
            {
                var minVersion = clientOp.GetMinVersion();
                
                Assert.IsTrue(minVersion >= ClientSocket.Ver100);
                Assert.IsTrue(minVersion <= ClientSocket.CurrentProtocolVersion);
            }
        }

        /// <summary>
        /// Tests that <see cref="ClientOpExtensions.GetMinVersion"/> returns a specific version for known new features.
        /// </summary>
        [Test]
        public void TestGetMinVersionReturnsSpecificVersionForNewFeatures()
        {
            Assert.AreEqual(ClientSocket.Ver140, ClientOp.CachePartitions.GetMinVersion());

            Assert.AreEqual(ClientSocket.Ver150, ClientOp.ClusterIsActive.GetMinVersion());
            Assert.AreEqual(ClientSocket.Ver150, ClientOp.ClusterChangeState.GetMinVersion());
            Assert.AreEqual(ClientSocket.Ver150, ClientOp.ClusterChangeWalState.GetMinVersion());
            Assert.AreEqual(ClientSocket.Ver150, ClientOp.ClusterGetWalState.GetMinVersion());
        }
    }
}