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

namespace Apache.Ignite.Core.Tests.Impl.Compute
{
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Compute;
    using Moq;
    using NUnit.Framework;

    /// <summary>
    /// Tests for the compute implementations
    /// </summary>
    [TestFixture]
    internal class ComputeImplTest
    {
        private const int OpWithNoResultCacheType = 9;

        /// <summary>
        /// Test caching was disabled by passing right type
        /// </summary>
        [Test]
        public void TestCachingWasDisabledByPassingRightType()
        {
            var target = GetTarget();
            var clusterGroupImpl = new ClusterGroupImpl(target.Object, null);
            var sut = new ComputeImpl(target.Object, clusterGroupImpl, true);

            sut.WithNoResultCache();

            target.Verify(x => x.InLongOutLong(OpWithNoResultCacheType, It.IsAny<long>()), Times.Once());
        }

        private static Mock<IPlatformTargetInternal> GetTarget()
        {
            var target = new Mock<IPlatformTargetInternal>();
            target
                .Setup(x => x.InLongOutLong(It.IsAny<int>(), It.IsAny<long>()))
                .Returns(1L);

            target
                .SetupGet(x => x.Marshaller)
                .Returns(new Marshaller(new BinaryConfiguration()));

            return target;
        }
    }
}