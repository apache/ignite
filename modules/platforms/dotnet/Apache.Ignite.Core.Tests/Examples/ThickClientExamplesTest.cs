namespace Apache.Ignite.Core.Tests.Examples
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests thick examples.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThickClientExamplesTest
    {
        /** */
        private static readonly Example[] ThickClientExamples = Example.AllExamples
            .Where(e => e.IsClient)
            .ToArray();

        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration(name: "server"));
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
        /// Tests thick mode example.
        /// </summary>
        [Test, TestCaseSource(nameof(ThickClientExamples))]
        public void TestThickExample(Example example)
        {
            // TODO: Verify required output ("example started", "example finished").
            Assert.IsFalse(example.IsThin);

            example.Run();
        }
    }
}
