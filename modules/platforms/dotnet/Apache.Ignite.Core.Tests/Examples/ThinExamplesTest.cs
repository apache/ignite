namespace Apache.Ignite.Core.Tests.Examples
{
    using System.Linq;
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
            Ignition.Start(TestUtils.GetTestConfiguration());
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
