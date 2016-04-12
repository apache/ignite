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

#pragma warning disable 618  // SpringConfigUrl
namespace Apache.Ignite.Core.Tests.Compute
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Cache.Query.Continuous;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests query in a cluster with Java-only and .NET nodes.
    /// </summary>
    public class MixedClusterQueryTest
    {
        /** */
        private const string SpringConfig = @"Config\Compute\compute-grid1.xml";

        /** */
        private const string SpringConfig2 = @"Config\Compute\compute-grid2.xml";

        /** */
        private const string StartTask = "org.apache.ignite.platform.PlatformStartIgniteTask";

        /** */
        private const string StopTask = "org.apache.ignite.platform.PlatformStopIgniteTask";

        /// <summary>
        /// Test.
        /// </summary>
        [Test]
        public void Test()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration()) {SpringConfigUrl = SpringConfig};

            using (var ignite = Ignition.Start(cfg))
            {
                var javaNodeName = ignite.GetCompute().ExecuteJavaTask<string>(StartTask, SpringConfig2);

                try
                {
                    Assert.IsTrue(ignite.WaitTopology(2));

                    TestJavaObjects(ignite);
                }
                finally
                {
                    ignite.GetCompute().ExecuteJavaTask<object>(StopTask, javaNodeName);
                }
            }
        }

        /// <summary>
        /// Tests the java objects.
        /// </summary>
        [SuppressMessage("ReSharper", "PossibleNullReferenceException")]
        private static void TestJavaObjects(IIgnite ignite)
        {
            var cache = ignite.GetOrCreateCache<int, string>("qry");

            var pred = JavaObjectFactory.CreateCacheEntryEventFilter<int, string>(
                "org.apache.ignite.platform.PlatformCacheEntryEventFilter",
                new Dictionary<string, object> {{"startsWith", "valid"}});

            var qry = new ContinuousQuery<int, string>(new QueryListener(), pred);

            using (cache.QueryContinuous(qry))
            {
                // Run on many keys to test all nodes
                for (var i = 0; i < 200; i++)
                {
                    QueryListener.Event = null;
                    cache[i] = "validValue";
                    Assert.AreEqual(cache[i], QueryListener.Event.Value);

                    QueryListener.Event = null;
                    cache[i] = "invalidValue";
                    Assert.IsNull(QueryListener.Event);
                }
            }
        }

        /// <summary>
        /// Test listener.
        /// </summary>
        private class QueryListener : ICacheEntryEventListener<int, string>
        {
            /** */
            public static volatile ICacheEntryEvent<int, string> Event;

            /** <inheritdoc /> */
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, string>> evts)
            {
                Event = evts.FirstOrDefault();
            }
        }
    }
}
