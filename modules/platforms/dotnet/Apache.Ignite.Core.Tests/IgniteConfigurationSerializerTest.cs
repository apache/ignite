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
    using System.Linq;
    using System.Xml;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Discovery;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Lifecycle;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IgniteConfiguration"/> serialization.
    /// </summary>
    public class IgniteConfigurationSerializerTest
    {
        [Test]
        public void Test()
        {
            var xml = @"<igniteConfig workDirectory='c:' JvmMaxMemoryMb='1024' MetricsLogFrequency='0:0:10'>
                            <localHost>127.1.1.1</localHost>
                            <binaryConfiguration>
                                <defaultNameMapper type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+NameMapper, Apache.Ignite.Core.Tests' bar='testBar' />
                            </binaryConfiguration>
                            <discoveryConfiguration joinTimeout='0:1:0'>
                                <ipFinder type='MulticastIpFinder' addressRequestAttempts='7' />
                            </discoveryConfiguration>
                            <jvmOptions><string>-Xms1g</string><string>-Xmx4g</string></jvmOptions>
                            <lifecycleBeans>
                                <iLifecycleBean type='Apache.Ignite.Core.Tests.IgniteConfigurationSerializerTest+LifecycleBean, Apache.Ignite.Core.Tests' foo='15' />
                            </lifecycleBeans>
                            <cacheConfiguration>
                                <cacheConfiguration cacheMode='Replicated'>
                                    <queryEntities>    
                                        <queryEntity keyType='System.Int32' valueType='System.String'>    
                                            <fields>
                                                <queryField name='length' type='System.Int32' />
                                            </fields>
                                        </queryEntity>
                                    </queryEntities>
                                </cacheConfiguration>
                            </cacheConfiguration>
                        </igniteConfig>";
            var reader = XmlReader.Create(new StringReader(xml));

            var cfg = IgniteConfigurationXmlSerializer.Deserialize(reader);

            Assert.AreEqual("c:", cfg.WorkDirectory);
            Assert.AreEqual("127.1.1.1", cfg.LocalHost);
            Assert.AreEqual(1024, cfg.JvmMaxMemoryMb);
            Assert.AreEqual(TimeSpan.FromSeconds(10), cfg.MetricsLogFrequency);
            Assert.AreEqual(TimeSpan.FromMinutes(1), cfg.DiscoveryConfiguration.JoinTimeout);
            Assert.AreEqual(7, ((MulticastIpFinder) cfg.DiscoveryConfiguration.IpFinder).AddressRequestAttempts);
            Assert.AreEqual(new[] { "-Xms1g", "-Xmx4g" }, cfg.JvmOptions);
            Assert.AreEqual(15, ((LifecycleBean) cfg.LifecycleBeans.Single()).Foo);
            Assert.AreEqual("testBar", ((NameMapper) cfg.BinaryConfiguration.DefaultNameMapper).Bar);
        }

        public class LifecycleBean : ILifecycleBean
        {
            public int Foo { get; set; }

            public void OnLifecycleEvent(LifecycleEventType evt)
            {
                // No-op.
            }
        }

        public class NameMapper : IBinaryNameMapper
        {
            public string Bar { get; set; }

            public string GetTypeName(string name)
            {
                return name;
            }

            public string GetFieldName(string name)
            {
                return name;
            }
        }
    }
}
