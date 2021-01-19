namespace Apache.Ignite.Core.Tests.Examples
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests thick examples.
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThickExamplesExternalNodeTest
    {
        /** */
        private static readonly Example[] ThickExamples = Example.AllExamples
            .Where(e => !e.IsThin && !e.IsClient && e.RequiresExternalNode)
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
        [Test, TestCaseSource(nameof(ThickExamples))]
        public void TestThickExampleWithExternalNode(Example example)
        {
            Assert.IsFalse(example.IsThin);
            Assert.IsTrue(example.RequiresExternalNode);

            example.Run();
        }
    }
}
