namespace Apache.Ignite.Core.Tests.Examples
{
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client example
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThinExamplesTest
    {
        /** */
        private static readonly Example[] ThinExamples = Example.AllExamples.Where(e => e.IsThin).ToArray();

        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            // Init default services.
            var sharedProj = Directory.GetFiles(ExamplePaths.SourcesPath, "*.csproj", SearchOption.AllDirectories)
                .Single(x => x.EndsWith("Shared.csproj"));

            var asmFile = Path.Combine(Path.GetDirectoryName(sharedProj), "bin", "Debug", "netcoreapp2.1", "Shared.dll");
            var asm = Assembly.LoadFrom(asmFile);
            var utils = asm.GetType("IgniteExamples.Shared.Utils");

            utils.InvokeMember(
                "DeployDefaultServices",
                BindingFlags.Static | BindingFlags.Public | BindingFlags.InvokeMethod,
                null,
                null,
                new object[] {ignite});
        }

        /// <summary>
        /// Tears down the fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the thin client example
        /// </summary>
        [Test, TestCaseSource(nameof(ThinExamples))]
        public void TestThinExample(Example example)
        {
            Assert.IsTrue(example.IsThin);

            example.Run();
        }
    }
}
