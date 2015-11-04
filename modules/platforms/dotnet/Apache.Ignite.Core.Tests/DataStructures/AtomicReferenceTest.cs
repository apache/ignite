namespace Apache.Ignite.Core.Tests.DataStructures
{
    using System.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Atomic reference test.
    /// </summary>
    public class AtomicReferenceTest : IgniteTestBase
    {
        /** */
        private const string AtomicRefName = "testAtomicRef";

        /// <summary>
        /// Initializes a new instance of the <see cref="AtomicReferenceTest"/> class.
        /// </summary>
        public AtomicReferenceTest() : base("config\\compute\\compute-grid1.xml")
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public override void TestSetUp()
        {
            base.TestSetUp();

            // Close test atomic if there is any
            Grid.GetAtomicReference(AtomicRefName, 0, true).Close();
        }

        /// <summary>
        /// Tests lifecycle of the AtomicReference.
        /// </summary>
        [Test]
        public void TestCreateClose()
        {
            Assert.IsNull(Grid.GetAtomicReference(AtomicRefName, 10, false));

            // Nonexistent atomic returns null
            Assert.IsNull(Grid.GetAtomicReference(AtomicRefName, 10, false));

            // Create new
            var al = Grid.GetAtomicReference(AtomicRefName, 10, true);
            Assert.AreEqual(AtomicRefName, al.Name);
            Assert.AreEqual(10, al.Get());
            Assert.AreEqual(false, al.IsClosed());

            // Get existing with create flag
            var al2 = Grid.GetAtomicReference(AtomicRefName, 5, true);
            Assert.AreEqual(AtomicRefName, al2.Name);
            Assert.AreEqual(10, al2.Get());
            Assert.AreEqual(false, al2.IsClosed());

            // Get existing without create flag
            var al3 = Grid.GetAtomicReference(AtomicRefName, 5, false);
            Assert.AreEqual(AtomicRefName, al3.Name);
            Assert.AreEqual(10, al3.Get());
            Assert.AreEqual(false, al3.IsClosed());

            al.Close();

            Assert.AreEqual(true, al.IsClosed());
            Assert.AreEqual(true, al2.IsClosed());
            Assert.AreEqual(true, al3.IsClosed());

            Assert.IsNull(Grid.GetAtomicReference(AtomicRefName, 10, false));
        }

        /// <summary>
        /// Tests modification methods.
        /// </summary>
        [Test]
        public void TestModify()
        {
            var atomics = Enumerable.Range(1, 10)
                .Select(x => Grid.GetAtomicReference(AtomicRefName, 5, true)).ToList();

            atomics.ForEach(x => Assert.AreEqual(5, x.Get()));

            atomics[0].Set(15);
            atomics.ForEach(x => Assert.AreEqual(15, x.Get()));

            Assert.AreEqual(15, atomics[0].CompareExchange(42, 15));
            atomics.ForEach(x => Assert.AreEqual(42, x.Get()));
        }
    }
}