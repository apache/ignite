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
