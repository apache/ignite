package org.apache.ignite.transactions.spring;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

public abstract class GridSpringTransactionManagerAbstractTest extends GridCommonAbstractTest {

    /** */
    protected static final String CACHE_NAME = "testCache";

    /** */
    public abstract IgniteCache<Integer, String> cache();

    /** */
    public abstract GridSpringTransactionService service();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().removeAll();
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "testGrid";
    }

    /** */
    public void testSuccessPut() {
        int entryCnt = 1_000;

        service().put(cache(), entryCnt);

        assertEquals(entryCnt, cache().size());
    }

    /** */
    public void testFailPut() {
        int entryCnt = 1_000;

        try {
            service().putWithError(cache(), entryCnt);
        }
        catch (NumberFormatException ignored) {
            System.out.println();
            // No-op.
        }

        assertEquals(0, cache().size());
    }

    /** */
    public void testMandatoryPropagation() {
        try {
            service().putWithMandatoryPropagation(cache());
        }
        catch (IllegalTransactionStateException e) {
            assertEquals("No existing transaction found for transaction marked with propagation 'mandatory'", e.getMessage());
        }

        assertEquals(0, cache().size());
    }

    /** */
    public void testUnsupportedIsolationLevel() {
        try {
            service().putWithUnsupportedIsolationLevel(cache());
        }
        catch (InvalidIsolationLevelException e) {
            assertEquals("Ignite does not support READ_UNCOMMITTED isolation level.", e.getMessage());
        }

        assertEquals(0, cache().size());
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDoSetRollbackOnlyInExistingTransaction() throws Exception {
        SpringTransactionManager mngr = new SpringTransactionManager();
        mngr.setIgniteInstanceName(grid().name());
        mngr.onApplicationEvent(null);

        TransactionTemplate txTmpl = new TransactionTemplate(mngr);

        try {
            txTmpl.execute(new TransactionCallback<Object>() {
                @Override public Object doInTransaction(TransactionStatus status) {
                    cache().put(1, "1");

                    Transaction tx = grid().transactions().tx();

                    assertFalse(tx.isRollbackOnly());

                    try {
                        service().putWithError(cache(), 1_000);
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }

                    assertTrue(tx.isRollbackOnly());

                    return null;
                }
            });
        }
        catch (Exception ignored) {
            // No-op.
        }

        assertEquals(0, cache().size());
    }
}
