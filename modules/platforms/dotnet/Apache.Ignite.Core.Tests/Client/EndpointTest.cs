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
            var cfg = new IgniteClientConfiguration("");
        }

        [Test]
        public void GetEndpoints_ParsesPortsAndRanges()
        {
            var ipWithDefaultPort = Endpoint.GetEndpoints(new IgniteClientConfiguration("1.2.3.4")).Single();
            Assert.AreEqual("1.2.3.4", ipWithDefaultPort.Host);
            Assert.AreEqual(IgniteClientConfiguration.DefaultPort, ipWithDefaultPort.Port);

            var ipWithCustomPort = Endpoint.GetEndpoints(new IgniteClientConfiguration("1.2.3.4:678")).Single();
            Assert.AreEqual("1.2.3.4", ipWithCustomPort.Host);
            Assert.AreEqual(678, ipWithCustomPort.Port);

            var ipWithPortRange = Endpoint.GetEndpoints(new IgniteClientConfiguration("1.2.3.4:678..680")).Single();
            Assert.AreEqual("1.2.3.4", ipWithPortRange.Host);
            Assert.AreEqual(678, ipWithPortRange.Port);
            Assert.AreEqual(2, ipWithPortRange.PortRange);
        }
    }
}
