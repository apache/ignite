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

namespace Apache.Ignite.Core.Tests.ApiParity
{
    using System.Collections.Generic;
    using NUnit.Framework;

    /// <summary>
    /// Tests that .NET <see cref="IgniteConfiguration"/> has all properties from Java configuration APIs.
    /// </summary>
    public class IgniteConfigurationParityTest
    {
        /** Known property name mappings Java -> .NET. */
        private static readonly Dictionary<string, string> KnownMappings = new Dictionary<string, string>
        {
            {"GridLogger", "Logger"},
            {"IncludeEventTypes", "IncludedEventTypes"},
        };

        /** Properties that are not needed on .NET side. */
        private static readonly string[] UnneededProperties =
        {
            "PeerClassLoadingThreadPoolSize",
            "IgfsThreadPoolSize",
            "UtilityCacheKeepAliveTime",
            "MBeanServer",
            "isPeerClassLoadingEnabled",
            "isMarshalLocalJobs",
            "PeerClassLoadingLocalClassPathExclude",
            "LifecycleBeans",
            "SslContextFactory",
            "SegmentationResolvers",
            "CollisionSpi",
            "DeploymentSpi",
            "CheckpointSpi",
            "FailoverSpi",
            "LoadBalancingSpi",
            "IndexingSpi",
            "AddressResolver",
            "DeploymentMode",
            "PeerClassLoadingMissedResourcesCacheSize",
            "CacheKeyConfiguration",
            "FileSystemConfiguration",
            "HadoopConfiguration",
            "ConnectorConfiguration",
            "ServiceConfiguration",
            "WarmupClosure",
            "ClassLoader",
            "CacheStoreSessionListenerFactories",
            "PlatformConfiguration",
            "ExecutorConfiguration",
            "CommunicationFailureResolver"
        };

        /** Properties that are missing on .NET side. */
        private static readonly string[] MissingProperties =
        {
            "RebalanceThreadPoolSize",
            "SegmentationPolicy",
            "isWaitForSegmentOnStart",
            "isAllSegmentationResolversPassRequired",
            "SegmentationResolveAttempts",
            "SegmentCheckFrequency",
            "isCacheSanityCheckEnabled",
            "TimeServerPortBase",
            "TimeServerPortRange",
            "IncludeProperties",
            "isAutoActivationEnabled"  // IGNITE-7301
        };

        /// <summary>
        /// Tests the ignite configuration parity.
        /// </summary>
        [Test]
        public void TestIgniteConfiguration()
        {
            ParityTest.CheckConfigurationParity(
                @"modules\core\src\main\java\org\apache\ignite\configuration\IgniteConfiguration.java", 
                typeof(IgniteConfiguration),
                UnneededProperties,
                MissingProperties,
                KnownMappings);
        }
    }
}