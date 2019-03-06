/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
