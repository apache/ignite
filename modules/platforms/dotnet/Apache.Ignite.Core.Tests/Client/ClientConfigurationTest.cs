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
    using Apache.Ignite.Core.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests various ways of configuring Ignite thin client.
    /// </summary>
    public class ClientConfigurationTest
    {
        /// <summary>
        /// Tests the FromXml method.
        /// </summary>
        [Test]
        public void TestFromXml()
        {
            
        }

        /// <summary>
        /// Tests the ToXml method.
        /// </summary>
        [Test]
        public void TestToXml()
        {
            // Empty config.
            // Empty config
            Assert.AreEqual("<?xml version=\"1.0\" encoding=\"utf-16\"?>\r\n<igniteClientConfiguration " +
                            "xmlns=\"http://ignite.apache.org/schema/dotnet/IgniteClientConfigurationSection\" />",
                new IgniteClientConfiguration().ToXml());
        }
    }
}
