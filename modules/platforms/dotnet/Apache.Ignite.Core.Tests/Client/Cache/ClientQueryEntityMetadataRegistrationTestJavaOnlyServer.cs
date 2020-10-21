namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.IO;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="QueryEntity.KeyTypeName"/> and <see cref="QueryEntity.ValueTypeName"/>
    /// settings trigger binary metadata registration on cache start for the specified types.
    /// <para />
    /// Normally, binary metadata is registered in the cluster when an object of the given type is first serialized
    /// (for cache storage or other purposes - Services, Compute, etc).
    /// However, query engine requires metadata for key/value types on cache start, so an eager registration
    /// should be performed.
    /// </summary>
    public class ClientQueryEntityMetadataRegistrationTestJavaOnlyServer : ClientQueryEntityMetadataRegistrationTest
    {
        /** */
        private const string CacheName = "cache1";

        /** */
        private const string StartTask = "org.apache.ignite.platform.PlatformStartIgniteTask";

        /** */
        private const string StopTask = "org.apache.ignite.platform.PlatformStopIgniteTask";

        /** */
        private static readonly IgniteConfiguration TempConfig =TestUtils.GetTestConfiguration(name: "tmp");

        /** */
        private string _javaNodeName;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public override void FixtureSetUp()
        {
            var springConfig = Path.Combine("Config", "query-entity-metadata-registration.xml");

            using (var ignite = Ignition.Start(TempConfig))
            {
                _javaNodeName = ignite.GetCompute().ExecuteJavaTask<string>(StartTask, springConfig);
                Assert.IsTrue(ignite.WaitTopology(2, 5000));
            }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public override void FixtureTearDown()
        {
            using (var ignite = Ignition.Start(TempConfig))
            {
                ignite.GetCompute().ExecuteJavaTask<object>(StopTask, _javaNodeName);
                Assert.IsTrue(ignite.WaitTopology(1, 5000));
            }
        }
    }
}
