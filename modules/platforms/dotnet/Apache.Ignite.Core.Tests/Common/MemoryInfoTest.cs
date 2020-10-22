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

namespace Apache.Ignite.Core.Tests.Common
{
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="MemoryInfo"/>.
    /// </summary>
    public class MemoryInfoTest
    {
        /// <summary>
        /// Tests that cgroup limit can be always determined on Linux.
        /// </summary>
        [Test]
        public void TestMemoryInfoReturnsNonNullLimitOnLinux()
        {
            if (Os.IsWindows)
            {
                return;
            }

            Assert.IsNotNull(MemoryInfo.TotalPhysicalMemory);
            Assert.Greater(MemoryInfo.TotalPhysicalMemory, 655360);

            Assert.IsNotNull(MemoryInfo.MemoryLimit);
            Assert.Greater(MemoryInfo.MemoryLimit, 655360);

            Assert.IsNotNull(CGroup.MemoryLimitInBytes);
            Assert.Greater(CGroup.MemoryLimitInBytes, 655360);

            if (CGroup.MemoryLimitInBytes > MemoryInfo.TotalPhysicalMemory)
            {
                Assert.AreEqual(MemoryInfo.TotalPhysicalMemory, MemoryInfo.MemoryLimit,
                    "When cgroup limit is not set, memory limit is equal to physical memory amount.");
            }
            else
            {
                Assert.AreEqual(CGroup.MemoryLimitInBytes, MemoryInfo.MemoryLimit,
                    "When cgroup limit is set, memory limit is equal to cgroup limit.");
            }
        }
    }
}
