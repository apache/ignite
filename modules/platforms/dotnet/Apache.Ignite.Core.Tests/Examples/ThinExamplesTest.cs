namespace Apache.Ignite.Core.Tests.Examples
{
    using NUnit.Framework;

    /// <summary>
    /// Tests thin client example
    /// </summary>
    [Category(TestUtils.CategoryExamples)]
    public class ThinExamplesTest
    {
        /// <summary>
        /// Tests the thin client example
        /// </summary>
        [Test, TestCaseSource(nameof(Example.ThinExamples))]
        public void TestThinExample(Example example)
        {
            Assert.IsTrue(example.IsThin);

            example.Run();
        }
    }
}
