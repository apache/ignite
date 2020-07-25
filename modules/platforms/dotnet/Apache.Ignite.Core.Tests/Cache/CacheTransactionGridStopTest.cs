namespace Apache.Ignite.Core.Tests.Cache
{
    using System;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests that stopping grid does not prevent <see cref="ITransaction"/> from finalizing.
    /// </summary>
    public class CacheTransactionGridStopTest : TestBase
    {
        /// <summary>
        /// 
        /// </summary>
        [Test]
        public void Test()
        {
            Assert.DoesNotThrow(() =>
            {
                using (Ignite.GetTransactions().TxStart())
                {
                    Ignition.Stop(Ignite.Name, true);
                }
                GC.Collect();
                GC.WaitForPendingFinalizers();
            });
        }
    }
}