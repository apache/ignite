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

namespace Apache.Ignite.Core.Tests.Cache.Query.Continuous
{
    using NUnit.Framework;

    /// <summary>
    /// Tests for ocntinuous query when there are no backups.
    /// </summary>
    public abstract class ContinuousQueryNoBackupAbstractTest : ContinuousQueryAbstractTest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cacheName">Cache name.</param>
        protected ContinuousQueryNoBackupAbstractTest(string cacheName) : base(cacheName)
        {
            // No-op.
        }

        /// <summary>
        /// Test regular callback operations for local query.
        /// </summary>
        [Test]
        public void TestCallbackLocal()
        {
            CheckCallback(true);
        }

        /// <summary>
        /// Test binary filter logic.
        /// </summary>
        [Test]
        public void TestFilterBinarizableLocal()
        {
            CheckFilter(true, true);
        }

        /// <summary>
        /// Test serializable filter logic.
        /// </summary>
        [Test]
        public void TestFilterSerializableLocal()
        {
            CheckFilter(false, true);
        }

        /// <summary>
        /// Test non-serializable filter for local query.
        /// </summary>
        [Test]
        public void TestFilterNonSerializableLocal()
        {
            CheckFilterNonSerializable(true);
        }
    }
}
