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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.IO;
    using Apache.Ignite.Core.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests IgniteManager class.
    /// </summary>
    public class IgniteManagerTest
    {
        /// <summary>
        /// Tests home dir resolver.
        /// </summary>
        [Test]
        public void TestIgniteHome()
        {
            var env = Environment.GetEnvironmentVariable(IgniteManager.EnvIgniteHome);
            
            Environment.SetEnvironmentVariable(IgniteManager.EnvIgniteHome, null);

            try
            {
                Assert.IsTrue(Directory.Exists(IgniteManager.GetIgniteHome(null)));
            }
            finally
            {
                // Restore
                Environment.SetEnvironmentVariable(IgniteManager.EnvIgniteHome, env);
            }
        }
    }
}