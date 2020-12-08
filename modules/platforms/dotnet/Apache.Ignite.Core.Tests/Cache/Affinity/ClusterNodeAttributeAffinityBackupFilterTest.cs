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

// ReSharper disable ObjectCreationAsStatement
// ReSharper disable RedundantCast
// ReSharper disable RedundantExplicitParamsArrayCreation
namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ClusterNodeAttributeAffinityBackupFilter"/>.
    /// </summary>
    public class ClusterNodeAttributeAffinityBackupFilterTest
    {
        [Test]
        public void TestConstructorDisallowsNullOrEmptyAttributeNames()
        {
            Assert.Throws<ArgumentNullException>(
                () => new ClusterNodeAttributeAffinityBackupFilter((string[])null));

            Assert.Throws<ArgumentNullException>(
                () => new ClusterNodeAttributeAffinityBackupFilter((IEnumerable<string>)null));

            Action<Action> checkErr = act =>
            {
                StringAssert.StartsWith(
                    "'attributeNames' argument should not be null or empty",
                    Assert.Throws<ArgumentException>(() => act()).Message);
            };

            checkErr(() => new ClusterNodeAttributeAffinityBackupFilter());
            checkErr(() => new ClusterNodeAttributeAffinityBackupFilter(Enumerable.Empty<string>()));
            checkErr(() => new ClusterNodeAttributeAffinityBackupFilter(new string[0]));
        }
    }
}
