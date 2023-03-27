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
    using System.Linq;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="Endpoint"/> class.
    /// </summary>
    public class EndpointTest
    {
        [Test]
        public void GetEndpoints_InvalidConfigFormat_ThrowsIgniteClientException()
        {
            var ex = AssertThrowsClientException("");
            Assert.AreEqual("IgniteClientConfiguration.Endpoints[...] can't be null or whitespace.", ex.Message);

            ex = AssertThrowsClientException("host:");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: host:",
                ex.Message);

            ex = AssertThrowsClientException("host:port");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: host:port",
                ex.Message);

            ex = AssertThrowsClientException("host:1..");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint, failed to parse port: host:1..",
                ex.Message);

            ex = AssertThrowsClientException("host:1..2..3");
            Assert.AreEqual(
                "Unrecognized format of IgniteClientConfiguration.Endpoint: host:1..2..3",
                ex.Message);
        }

        [Test]
        public void GetEndpoints_ParsesPortsAndRanges()
        {
            const string ip = "1.2.3.4";
            const string host = "example.com";
            const int port = 678;
            const int port2 = 680;

            var ipWithDefaultPort = Endpoint.GetEndpoints(new IgniteClientConfiguration(ip)).Single();
            Assert.AreEqual(ip, ipWithDefaultPort.Host);
            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, ipWithDefaultPort.Port);
            Assert.AreEqual(0, ipWithDefaultPort.PortRange);

            var ipWithCustomPort = Endpoint
                .GetEndpoints(new IgniteClientConfiguration(string.Format("{0}:{1}", ip, port)))
                .Single();
            Assert.AreEqual(ip, ipWithCustomPort.Host);
            Assert.AreEqual(port, ipWithCustomPort.Port);
            Assert.AreEqual(0, ipWithCustomPort.PortRange);

            var ipWithPortRange = Endpoint
                .GetEndpoints(new IgniteClientConfiguration(string.Format("{0}:{1}..{2}", ip, port, port2)))
                .Single();
            Assert.AreEqual(ip, ipWithPortRange.Host);
            Assert.AreEqual(port, ipWithPortRange.Port);
            Assert.AreEqual(port2 - port, ipWithPortRange.PortRange);

            var hostWithDefaultPort = Endpoint.GetEndpoints(new IgniteClientConfiguration(host)).Single();
            Assert.AreEqual(host, hostWithDefaultPort.Host);
            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, hostWithDefaultPort.Port);
            Assert.AreEqual(0, hostWithDefaultPort.PortRange);

            var hostWithCustomPort = Endpoint
                .GetEndpoints(new IgniteClientConfiguration(string.Format("{0}:{1}", host, port)))
                .Single();
            Assert.AreEqual(host, hostWithCustomPort.Host);
            Assert.AreEqual(port, hostWithCustomPort.Port);
            Assert.AreEqual(0, hostWithCustomPort.PortRange);

            var hostWithPortRange = Endpoint
                .GetEndpoints(new IgniteClientConfiguration(string.Format("{0}:{1}..{2}", host, port, port2)))
                .Single();
            Assert.AreEqual(host, hostWithPortRange.Host);
            Assert.AreEqual(port, hostWithPortRange.Port);
            Assert.AreEqual(port2 - port, hostWithPortRange.PortRange);

        }

        private static IgniteClientException AssertThrowsClientException(string endpoint)
        {
            var endpoints = Endpoint.GetEndpoints(new IgniteClientConfiguration(endpoint));

            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            return Assert.Throws<IgniteClientException>(() => endpoints.ToList());
        }
    }
}
