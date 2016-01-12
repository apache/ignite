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
    using NUnit.Framework;

    /// <summary>
    /// Client reconnect tests.
    /// </summary>
    public class ReconnectTest
    {
        [Test]
        public void TestDisconnectedException()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = "config\\compute\\compute-grid1.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions()
            };

            Ignition.ClientMode = true;

            using (var ignite = Ignition.Start(cfg))
            {
                
            }
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.ClientMode = false;
        }
    }
}
