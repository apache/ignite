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

namespace Apache.Ignite.Core.Tests.DotNetCore.Common
{
    using System;
    using Apache.Ignite.Core.Tests.Cache.Affinity;

    /// <summary>
    /// Test runner.
    /// </summary>
    internal class TestRunner
    {
        /// <summary>
        /// Console entry point.
        /// </summary>
        private static void Main(string[] args)
        {
            var t1 = new AffinityFunctionSpringTest();
            t1.TestSetUp();
            t1.TestDynamicCache();
            t1.TestTearDown();
            
            t1.TestSetUp();
            t1.TestStaticCache();
            t1.TestTearDown();
            t1.FixtureTearDown();
            
            var t2 = new AffinityFunctionTest();
            t2.FixtureSetUp();
            t2.TestDynamicCache();
            t2.TestDynamicCachePredefined();
            t2.TestExceptionInFunction();
            t2.TestInheritRendezvousAffinity();
            t2.TestRemoveNode();
            t2.TestSimpleInheritance();
            t2.TestStaticCache();
            Console.WriteLine(">>> TEARDOWN");
            t2.FixtureTearDown();
            Console.WriteLine(">>> END");
        }
    }
}